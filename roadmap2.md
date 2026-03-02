1. 验收标准更加详细，具体到完整的crud流程和格式化输出示例
2. 提供完整的功能checklist
3. 测试策略、性能目标、
4. 技术选型论证、分层架构思想、包结构规划
5. 

# MiniDB 数据库内核开发全景指南 (Technical Design Document)

> **核心哲学**：自底向上，层层封装。每一层都只通过接口与下一层交互，绝不越级访问。

## ✅ 阶段一：存储引擎内核 (Storage Kernel) - [已完成]

**定位**：操作系统的“文件”与数据库“Page”之间的搬运工。

- **已实现核心**：`DiskManager` (NIO), `BufferPoolManager` (Cache), `LRUReplacer`, `Page`.
- **当前能力**：可以按 PageId 随机读写 4KB 数据块，无需关心磁盘 IO 细节。

### 1. 文件结构规划

```
com.hunkyhsu.minidb
└── storage
    ├── DiskManager.java        # 负责读写磁盘文件
    ├── BufferPoolManager.java  # 负责内存页的管理与缓存
    ├── Page.java               # 内存中 4KB 页的抽象
    ├── LRUReplacer.java        # LRU 缓存置换算法实现
    └── Replacer.java           # 置换算法接口
```

### 2. 核心逻辑流程图

**获取页面 (Fetch Page):**

```
[BPM.fetchPage(pageId)]
      |
      v
[检查 pageTable] --(Hit)--> [Pin Page] -> [Replacer.pin] -> 返回 Page
      |
      v
(Miss) -> [Replacer.victim] --(无空位)--> [Throw Exception]
              |
              v
       (有 Victim) -> [Is Victim Dirty?] --(Yes)--> [DiskManager.writePage]
              |
              v
       [DiskManager.readPage(pageId)] -> [Update pageTable] -> 返回 Page
```

### 3. 技术选型与权衡 (Design Rationale)

- **Q: DiskManager 为何选择 Java NIO (`FileChannel`) 而非标准 IO (`RandomAccessFile`)?**
  - **选择**: NIO `FileChannel` + `ByteBuffer` (Direct Buffer)。
  - **原因**:
    1. **性能**: `FileChannel` 可以直接利用操作系统的 Page Cache，且 `DirectBuffer` 支持零拷贝（Zero-Copy），减少了内核态到用户态的数据复制。
    2. **线程安全**: `FileChannel` 是线程安全的，支持并发读写文件的不同位置（虽然我们目前上层加了锁，但这为未来优化留了空间）。
- **Q: Page 内存分配为何选择 `allocatePage` (Eager Allocation) 而非 Lazy Allocation?**
  - **选择**: Eager Allocation (分配 ID 时立即写入 4KB 空数据)。
  - **原因**:
    1. **Fail-Fast**: 如果磁盘已满，`allocatePage` 会立刻抛出异常，而不是等到用户写入数据时才崩溃。
    2. **防止稀疏文件**: 保证文件在磁盘上是连续分配的，避免某些文件系统对稀疏文件的性能惩罚。
- **Q: BufferPoolManager 为何选择全局锁 (`Global Lock`) 而非分段锁?**
  - **选择**: 单个 `ReentrantLock` 保护整个 BPM。
  - **原因**:
    1. **简单性**: 数据库内核开发初期，正确性 > 性能。全局锁最不容易死锁。
    2. **现状**: Java 的 `ConcurrentHashMap` 虽然是分段的，但 LRU 链表和 FreeList 的操作很难做到无锁或分段。在 Stage 1，全局锁是实现 LRU 最稳健的方案。

### 4. 验收任务与标准

- **任务**: 实现 BPM 的 `fetchPage`, `unpinPage`, `flushPage` 等核心方法。
- **标准**:
  - **Mental Check**: 能够写入数据 -> 强制驱逐 (Evict) -> 重新读取，数据内容不变。
  - **Concurrency**: 10 个线程并发随机读写，无死锁，无数据损坏。

## 🚀 阶段二：物理表结构 (Physical Table Access) - [当前阶段]

**定位**：将“裸字节”组织成“行记录”。解决如何在 4KB 空间内高效存储变长数据。

**核心数据结构**：Slotted Page (槽位页) & Linked List (页链表)。

### 1. 文件结构规划

```
com.hunkyhsu.minidb
└── storage
    └── table
        ├── RecordId.java       # 物理指针 (PageId, SlotId)
        ├── Tuple.java          # 数据行容器 (目前只存 byte[] data)
        ├── TablePage.java      # [核心] 继承 Page，实现 Slotted Page 布局位操作
        ├── TableHeap.java      # [核心] 表的入口，管理 Page 双向链表
        └── TableIterator.java  # 全表扫描迭代器
```

### 2. 核心技术细节：Slotted Page 布局

```
----------------------------------------------------------------
| Header (24B) | Slot Array (从左往右) --> | <-- Data (从右往左) |
----------------------------------------------------------------
Header:
[0-4]   PageId
[4-8]   PrevPageId
[8-12]  NextPageId
[12-16] FreeSpacePointer (指向 Data 的起始位置)
[16-20] TupleCount
[20-24] LSN (Log Sequence Number) - Stage 5 使用
```

### 3. 技术选型与权衡 (Design Rationale)

- **Q: 为什么选择 Slotted Page 结构而非 Log-Structured 或简单的 Append-Only?**
  - **选择**: Slotted Page。
  - **原因**:
    1. **定长指针**: `Slot Array` 使得外部引用（RecordId）可以固定为 `(PageId, SlotId)`。即使内部进行碎片整理（Compaction）移动了 Tuple 数据，只要更新 Slot 指针，外部 RID 不用变。
    2. **变长支持**: 完美支持变长数据（Varchar），空间利用率高。
- **Q: 插入 Tuple 时，Slot ID 是否复用（Reuse）?**
  - **选择**: 不复用 (Append-Only Slots)。
  - **原因**:
    1. **悬挂指针 (Dangling Pointer)**: 如果复用 Slot ID，旧的索引可能指向新插入的无关数据，导致严重的数据错误。
    2. **MVCC**: 在多版本并发控制下，旧版本可能还需要被访问。
- **Q: `TableHeap` 为什么是双向链表?**
  - **选择**: Double Linked List (Prev + Next)。
  - **原因**: 为了支持未来的 **全表逆向扫描** 以及 **页面回收/合并**（需要修改前驱节点的 Next 指针）。

### 4. 验收任务与标准

- **任务 1**: 实现 `TablePage` 的位操作（Header, Slot Array, FreeSpace 维护）。
- **任务 2**: 实现 `TableHeap` 的跨页插入（Page 满自动扩容）。
- **任务 3**: 编写单元测试，插入 10,000 条变长数据（如随机字符串）。
- **验收标准**:
  - 能够通过 Iterator 完整读出 10,000 条数据，顺序一致，内容无损。
  - 磁盘文件大小应随数据量自动增长（约 40MB+）。

## 🧩 阶段三：类型系统与元数据 (Type System & Catalog)

**定位**：赋予字节以“语义”。数据库开始理解 `int` vs `varchar`。

**关键转变**：从操作 `byte[]` 变为操作 `List<Value>`。

### 1. 文件结构规划

```
com.hunkyhsu.minidb
├── type
│   ├── Type.java           # 枚举：INTEGER, VARCHAR, BOOLEAN
│   ├── Value.java          # 数据基类 (能够 serialize/deserialize)
│   ├── IntegerType.java
│   └── StringType.java
├── catalog
│   ├── Column.java         # 列定义 (name, type, offset)
│   ├── Schema.java         # 表结构定义 (List<Column>)
│   └── TableInfo.java      # 表元数据 (Schema + TableHeap实例)
└── Catalog.java            # 全局元数据管理器 (Map<String, TableInfo>)
```

### 2. 技术选型与权衡 (Design Rationale)

- **Q: 类型系统采用 Static Typing 还是 Dynamic Typing?**
  - **选择**: Static Typing (Schema-based)。
  - **原因**:
    1. **紧凑存储**: 因为 Schema 已知，Tuple 内部不需要存 Type ID 或 Column Name，只存纯数据，极大节省空间。
    2. **性能**: 解析时直接根据 Schema 的 offset 读取，无需动态判断类型。
- **Q: Catalog 如何持久化?**
  - **选择**: 暂时不持久化 (Bootstrap with Hardcoded Schema) 或 序列化为 JSON 存文件。
  - **原因**: 实现一个“存自己的表来存表信息”（System Catalog Tables）非常复杂（递归依赖）。在初期，使用简单的文件序列化（如 `catalog.json`）是最务实的做法。

### 3. 验收任务与标准

- **任务**: 实现 `Catalog` 管理表名到 `TableHeap` 的映射。
- **验收标准**: 创建一个 `User(id int, name varchar)` 表，插入 `(1, "minidb")`，读取出来必须能用 `getValue(0)` 得到整数 1。

## ⚙️ 阶段四：查询执行引擎 (Query Execution)

**定位**：数据处理的大脑。支持 SQL 算子。

**架构**：火山模型 (Volcano Model / Iterator Model)。

### 1. 文件结构规划

```
com.hunkyhsu.minidb.execution
├── executors
│   ├── AbstractExecutor.java # 接口: init(), next()
│   ├── SeqScanExecutor.java  # 顺序扫描
│   ├── InsertExecutor.java   # 插入
│   ├── FilterExecutor.java   # WHERE 过滤
│   └── ProjectExecutor.java  # SELECT 投影
└── ExecutionContext.java     # 执行上下文 (持有 Transaction, Catalog)
```

### 2. 技术选型与权衡 (Design Rationale)

- **Q: 为什么选择 Volcano Model (Iterator Model)?**
  - **选择**: 每个算子实现 `next()` 方法，一次返回一个 Tuple。
  - **原因**:
    1. **内存友好**: 不需要将所有数据加载到内存，适合处理超过内存大小的数据集（Pipeline Streaming）。
    2. **实现简单**: 算子之间通过统一接口耦合，组合灵活（如 `Filter(SeqScan)`）。
- **Q: Filter 表达式如何求值?**
  - **选择**: 简单的解释器模式 (Interpreter Pattern) 或 表达式树 (Expression Tree)。
  - **原因**: 相比于 JIT 编译（如 LLVM），解释执行实现简单，足以应对教学场景。

### 3. 验收任务与标准

- **任务**: 手动构建执行计划树。
- **验收标准**: 能够执行 `INSERT` 算子写入数据，随后用 `SEQ_SCAN + FILTER` 查出符合条件的数据。

## 🔒 阶段五：事务与并发控制 (Transaction & Recovery)

**定位**：实现 ACID。这是最难、最复杂的阶段。

**核心**：锁管理器 (2PL) + 预写日志 (WAL)。

### 1. 文件结构规划

```
com.hunkyhsu.minidb
├── concurrency
│   ├── LockManager.java      # 锁管理器 (S-Lock, X-Lock)
│   └── Transaction.java      # 事务状态维护
├── recovery
│   ├── LogManager.java       # 日志读写
│   ├── LogRecord.java        # 日志格式 (LSN, PrevLSN, Type, Data)
│   └── CheckpointManager.java
```

### 2. 技术选型与权衡 (Design Rationale)

- **Q: 并发控制选择 2PL 还是 MVCC?**
  - **选择**: **MVCC + 2PL** (Hybrid)。
  - **原因**:
    1. **读写不阻塞**: MVCC 允许读操作（快照读）不加锁，极大提高并发读性能。
    2. **写写互斥**: 写入操作依然通过 LockManager (2PL) 进行悲观加锁，保证数据一致性。
- **Q: 恢复算法选择 ARIES 吗?**
  - **选择**: 简化版 ARIES (Redo + Undo)。
  - **原因**: ARIES 是事实标准。我们需要实现 LSN (Log Sequence Number) 来保证日志的顺序，以及 Checkpoint 来缩短恢复时间。

### 3. 验收任务与标准

- **并发测试**: 10 个线程同时修改同一张表，最终数据必须一致（无丢失更新）。
- **恢复测试**: 在写入过程中强制 Kill 进程，重启后数据库能通过 WAL 恢复未刷盘的数据。

## 🌐 阶段六：SQL 解析与网络服务 (Interface)

**定位**：包装成真正的数据库服务器。

### 1. 核心任务

- **Parser**: 引入 `JSqlParser`，将 SQL 字符串转为 AST。
- **Binder**: 将 AST 绑定到 Catalog（验证表是否存在）。
- **Network**: 使用 Netty 监听端口，解析 MySQL 协议包，调用 Execution Engine。

### 2. 技术选型与权衡 (Design Rationale)

- **Q: 网络协议选择自定义还是兼容 MySQL?**
  - **选择**: 尽量兼容 MySQL Protocol (Handshake + Command Packet)。
  - **原因**: 可以直接使用现有的 MySQL 客户端（如 Navicat, MySQL Workbench）连接我们的数据库，极大地提升可用性和成就感。