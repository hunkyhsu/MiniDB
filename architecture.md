# MiniDB 系统架构设计

## 整体架构图

```
┌─────────────────────────────────────────────────┐
│           SQL 接口层（阶段四）                    │
│  NettyServer + JSqlParser + Protocol Handler   │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│         查询执行层（阶段三.5）                    │
│  Executor Framework + Schema Manager + Catalog │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│      事务与并发控制层（阶段三）                   │
│  Transaction Manager + Lock Manager + WAL Log  │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│         数据访问层（阶段二）                      │
│  Tuple + TableHeap + SeqScan + Index (B+Tree)  │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│         存储引擎层（阶段一）                      │
│  Page + DiskManager + BufferPoolManager (LRU)  │
└─────────────────────────────────────────────────┘
```

---

## 核心组件详解

### 第一层：存储引擎层 (Storage Engine Layer)

**职责：** 管理磁盘 I/O 和内存缓存

#### 核心组件：

1. **Page**
   - 固定大小：4KB (4096 字节)
   - 页面类型：数据页、索引页、元数据页
   - 页面头部：PageID、页面类型、LSN（日志序列号）

2. **DiskManager**
   - 负责文件的读写操作
   - 管理页面在磁盘文件中的位置
   - API：`readPage(pageId)`, `writePage(pageId, data)`

3. **BufferPoolManager**
   - 管理内存中的页面缓存池
   - 实现 LRU 替换策略
   - Pin/Unpin 机制防止正在使用的页面被换出
   - 脏页管理和刷盘策略

---

### 第二层：数据访问层 (Data Access Layer)

**职责：** 提供行级数据访问和存储

#### 核心组件：

1. **Type System（类型系统）**
   ```java
   interface Type {
       byte[] serializeToBytes(Value value);
       Value deserializeFromBytes(byte[] data);
       int getFixedSize();
   }

   // 实现类
   - IntegerType (4 bytes)
   - BigIntType (8 bytes)
   - VarcharType (变长)
   - DateType (8 bytes)
   - DecimalType (变长)
   ```

2. **Value（值对象）**
   - 封装具体的数据值
   - 类型安全的访问接口

3. **Schema（模式定义）**
   - 描述一行数据的列定义
   - Column：name, type, nullable, default value

4. **Tuple（元组/行）**
   - 表示数据库中的一行数据
   - 使用 Schema 进行序列化/反序列化
   - RID (Record ID)：(PageID, SlotID)

5. **Slotted Page（槽页结构）**
   ```
   +------------------+
   | Page Header      | <- PageID, SlotCount, FreeSpacePointer
   +------------------+
   | Slot Array       | <- [(Offset, Length), ...]
   +------------------+
   | ... Free Space...|
   +------------------+
   | Tuple N          |
   | Tuple 2          |
   | Tuple 1          | <- 从页面底部向上增长
   +------------------+
   ```

6. **TableHeap**
   - 管理一个表的所有页面
   - 支持插入、删除、更新操作
   - 自动分配新页面

7. **Index (B+Tree)**
   - 支持快速查找
   - 主键索引（唯一索引）
   - 二级索引（可选）

---

### 第三层：事务与并发控制层 (Transaction Layer)

**职责：** 保证 ACID 特性

#### 核心组件：

1. **Transaction Manager**
   - 分配事务 ID (TxID)
   - 管理事务状态：Running, Committed, Aborted
   - 事务的开始、提交、回滚

2. **Lock Manager**
   - 实现两阶段锁协议（2PL）
   - 锁类型：Shared Lock (S), Exclusive Lock (X)
   - 锁粒度：行级锁（Record-level）
   - 死锁检测：超时或依赖图检测

3. **Log Manager (WAL)**
   - Write-Ahead Logging 协议
   - 日志类型：
     - BEGIN/COMMIT/ABORT
     - INSERT/DELETE/UPDATE
     - CLR（补偿日志记录）
   - 日志格式：`[LSN] [TxID] [Type] [PageID] [Data]`

4. **Recovery Manager**
   - ARIES 算法：
     - Analysis Phase（分析阶段）
     - Redo Phase（重做阶段）
     - Undo Phase（回滚阶段）
   - Checkpoint 机制

5. **Checkpoint Manager**
   - 定期刷新脏页到磁盘
   - 记录 Checkpoint LSN
   - 加速恢复过程

---

### 第四层：查询执行层 (Execution Layer)

**职责：** 将 SQL 转换为具体的执行操作

#### 核心组件：

1. **Catalog（系统目录）**
   - 管理元数据
   - 系统表：
     - `minidb_tables`：存储所有表的信息
     - `minidb_columns`：存储列定义
     - `minidb_indexes`：存储索引信息

2. **Schema Manager**
   - CREATE TABLE / DROP TABLE
   - ALTER TABLE（可选）
   - 元数据持久化

3. **Executor Framework（火山模型）**
   ```java
   interface Executor {
       void init();
       Tuple next();  // 返回下一条结果，无结果返回 null
       void close();
   }
   ```

4. **执行器类型**
   - **SeqScanExecutor**：全表扫描
   - **IndexScanExecutor**：索引扫描
   - **FilterExecutor**：WHERE 条件过滤
   - **ProjectionExecutor**：SELECT 列投影
   - **InsertExecutor**：INSERT 语句
   - **DeleteExecutor**：DELETE 语句
   - **UpdateExecutor**：UPDATE 语句
   - **NestedLoopJoinExecutor**：JOIN 操作（可选）
   - **AggregateExecutor**：聚合函数（可选）

5. **Planner（查询计划器）**
   - 将 SQL 转换为执行计划树
   - 简单的优化策略（如索引选择）

---

### 第五层：SQL 接口层 (SQL Interface Layer)

**职责：** 提供网络访问和 SQL 解析

#### 核心组件：

1. **NettyServer**
   - 监听指定端口（默认 8888）
   - 处理客户端连接
   - 会话管理

2. **Protocol Handler（通信协议）**
   ```
   请求格式：
   +------------+
   | SQL Length | (4 bytes, int)
   +------------+
   | SQL String | (variable length)
   +------------+

   响应格式：
   +------------+
   | Status     | (1 byte: 0=Success, 1=Error)
   +------------+
   | Row Count  | (4 bytes, int)
   +------------+
   | Schema     | (variable: column names + types)
   +------------+
   | Row Data   | (variable: serialized tuples)
   +------------+
   ```

3. **SQL Parser**
   - 使用 JSqlParser 解析 SQL
   - 支持的语句：
     - DDL：CREATE TABLE, DROP TABLE
     - DML：INSERT, SELECT, UPDATE, DELETE
     - TCL：BEGIN, COMMIT, ROLLBACK

4. **Result Formatter**
   - 将执行结果格式化为表格
   - 支持不同的输出格式（文本、JSON）

---

## 数据流示例

### 示例 1：INSERT 语句执行流程

```
用户输入: INSERT INTO user VALUES (1, 'Alice');

1. SQL 接口层
   ├─ NettyServer 接收请求
   ├─ JSqlParser 解析 SQL
   └─ 生成 InsertPlan

2. 查询执行层
   ├─ InsertExecutor 初始化
   ├─ 获取 user 表的 TableHeap
   └─ 开启事务

3. 事务层
   ├─ 分配 TxID = 100
   ├─ 获取行级写锁
   └─ 写 WAL 日志: [LSN=500][TxID=100][INSERT][...]

4. 数据访问层
   ├─ TableHeap 分配 Slot
   ├─ Tuple 序列化为字节
   └─ 写入 Slotted Page

5. 存储引擎层
   ├─ BufferPool.fetchPage(pageId)
   ├─ 标记为脏页
   └─ 后台异步刷盘

6. 事务提交
   ├─ 写 COMMIT 日志
   ├─ 释放锁
   └─ 返回成功响应
```

### 示例 2：SELECT 语句执行流程

```
用户输入: SELECT * FROM user WHERE id = 1;

1. SQL 接口层
   ├─ JSqlParser 解析 SQL
   └─ 生成 SelectPlan

2. 查询执行层
   ├─ 构建执行树:
   │   ProjectionExecutor
   │         ↓
   │   FilterExecutor (id = 1)
   │         ↓
   │   IndexScanExecutor (如果有索引) 或 SeqScanExecutor
   └─ 调用 next() 迭代结果

3. 事务层
   ├─ 分配只读事务 TxID
   └─ 获取共享锁 (S-Lock)

4. 数据访问层
   ├─ IndexScan 通过 B+Tree 定位 RID
   ├─ TableHeap.getTuple(rid)
   └─ 反序列化 Tuple

5. 存储引擎层
   ├─ BufferPool.fetchPage(pageId)
   └─ 返回 Page 数据

6. 结果返回
   ├─ 格式化为表格
   └─ 通过 Netty 返回客户端
```

---

## 关键技术点

### 1. 并发控制

**两阶段锁协议 (2PL)：**
- 增长阶段：只能获取锁，不能释放
- 收缩阶段：只能释放锁，不能获取

**隔离级别：READ COMMITTED**
- 读操作获取 S-Lock，读完立即释放
- 写操作获取 X-Lock，事务提交时释放

### 2. 崩溃恢复

**WAL 协议：**
1. 修改前先写日志
2. 日志持久化后才能修改数据页
3. 事务提交前必须刷日志

**Checkpoint 机制：**
```
时间线: ... T1 T2 [Checkpoint] T3 T4 [Crash]
恢复时: 从 Checkpoint 开始，Redo T3 T4
```

### 3. 缓冲池管理

**LRU 替换策略：**
- 使用双向链表 + HashMap
- Pin Count > 0 的页面不能被换出
- 脏页换出前必须先刷盘

---

## MVP 功能范围

### ✅ 必须实现

- [x] Page 管理
- [x] BufferPool + LRU
- [x] DiskManager
- [x] 类型系统（INT, VARCHAR）
- [x] Tuple + TableHeap
- [x] Slotted Page
- [x] Schema 管理
- [x] Catalog 持久化
- [x] WAL 日志
- [x] Lock Manager
- [x] Transaction Manager
- [x] Recovery Manager
- [x] Checkpoint
- [x] B+Tree 主键索引
- [x] 执行器框架
- [x] INSERT / SELECT / UPDATE / DELETE
- [x] CREATE TABLE / DROP TABLE
- [x] NettyServer
- [x] 通信协议

### ❌ MVP 暂不实现

- [ ] JOIN 操作
- [ ] 子查询
- [ ] 聚合函数（GROUP BY, COUNT, SUM）
- [ ] 视图（VIEW）
- [ ] 存储过程
- [ ] 触发器
- [ ] 外键约束
- [ ] MVCC（多版本并发控制）
- [ ] 查询优化器
- [ ] 统计信息收集

---

## 性能指标

### 目标（单机）

- **吞吐量：** 1000+ TPS (Transactions Per Second)
- **延迟：** 平均 < 10ms (简单查询)
- **并发：** 支持 100+ 并发连接
- **恢复时间：** < 10s (对于 1GB 数据)

### 瓶颈分析

1. **磁盘 I/O**
   - 优化：BufferPool 命中率 > 95%
   - 优化：批量刷盘，减少随机 I/O

2. **锁竞争**
   - 优化：细粒度锁（行级锁）
   - 优化：读写锁分离

3. **日志写入**
   - 优化：Group Commit（批量提交）
   - 优化：使用 Direct I/O

---

## 代码组织结构

```
minidb/
├── src/main/java/com/minidb/
│   ├── storage/               # 存储引擎层
│   │   ├── page/
│   │   │   ├── Page.java
│   │   │   └── SlottedPage.java
│   │   ├── disk/
│   │   │   └── DiskManager.java
│   │   └── buffer/
│   │       ├── BufferPoolManager.java
│   │       └── LRUReplacer.java
│   │
│   ├── table/                 # 数据访问层
│   │   ├── type/
│   │   │   ├── Type.java
│   │   │   ├── IntegerType.java
│   │   │   └── VarcharType.java
│   │   ├── schema/
│   │   │   ├── Schema.java
│   │   │   └── Column.java
│   │   ├── tuple/
│   │   │   ├── Tuple.java
│   │   │   └── RID.java
│   │   └── heap/
│   │       └── TableHeap.java
│   │
│   ├── index/                 # 索引层
│   │   └── btree/
│   │       ├── BPlusTree.java
│   │       └── BPlusTreePage.java
│   │
│   ├── transaction/           # 事务层
│   │   ├── Transaction.java
│   │   ├── TransactionManager.java
│   │   ├── LockManager.java
│   │   └── LogManager.java
│   │
│   ├── recovery/              # 恢复层
│   │   ├── RecoveryManager.java
│   │   └── CheckpointManager.java
│   │
│   ├── execution/             # 执行层
│   │   ├── executor/
│   │   │   ├── Executor.java
│   │   │   ├── SeqScanExecutor.java
│   │   │   ├── IndexScanExecutor.java
│   │   │   ├── InsertExecutor.java
│   │   │   ├── DeleteExecutor.java
│   │   │   └── UpdateExecutor.java
│   │   └── plan/
│   │       ├── Plan.java
│   │       └── Planner.java
│   │
│   ├── catalog/               # 元数据管理
│   │   ├── Catalog.java
│   │   ├── TableMetadata.java
│   │   └── IndexMetadata.java
│   │
│   ├── network/               # 网络层
│   │   ├── NettyServer.java
│   │   ├── ProtocolHandler.java
│   │   └── ResultFormatter.java
│   │
│   └── common/                # 公共组件
│       ├── Config.java
│       └── exception/
│           └── MiniDBException.java
│
└── src/test/java/             # 测试代码
    └── com/minidb/
        ├── storage/
        ├── table/
        └── integration/
```

---

## 测试策略

### 单元测试

- 每个核心类都需要单元测试
- 覆盖率 > 80%

### 集成测试

1. **存储层测试**
   - Page 读写正确性
   - BufferPool LRU 替换
   - 崩溃后数据完整性

2. **事务层测试**
   - 并发插入/更新
   - 死锁检测
   - Rollback 正确性

3. **端到端测试**
   - 通过网络执行 SQL
   - 验证结果正确性

### 性能测试

- TPC-C Benchmark（简化版）
- 压力测试：1000 并发连接

---

## 参考资料

1. **CMU 15-445: Database Systems**
   - https://15445.courses.cs.cmu.edu/

2. **《数据库系统实现》（Database System Implementation）**
   - Hector Garcia-Molina

3. **《数据库系统概念》（Database System Concepts）**
   - Abraham Silberschatz

4. **《事务处理：概念与技术》**
   - Jim Gray

5. **开源项目参考**
   - SQLite (C)
   - Derby (Java)
   - BusTub (C++, CMU 教学项目)