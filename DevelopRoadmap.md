# MiniDB Develop Roadmap

> Hunky Hsu

## Content:

- Stage 1: Storage Engine
- Stage 2: Tuple & Scan
- Stage 3: Type system & Catalog
- Stage 4: Query Execution Engine & Volcano Model
- Stage 5: Concurrency & Recovery
- Stage 6: SQL Parser & SQL Interface

## Stage 1：Storage Engine Foundation

**目标**：实现 Page、DiskManager 和 BufferPool。 **核心逻辑**：不考虑 SQL，只考虑如何把“字节”存在磁盘，并缓存在内存。

### 1. 任务指标

- 定义 `Page` 结构：固定大小 4KB（4096 字节）。
- 实现 `DiskManager`：负责随机读写文件中的某个 Page。
- 实现 `BufferPoolManager`：管理内存中的 Page 数组，实现 LRU 替换算法。

### 2. 验收标准

- **代码表现**：你能通过 `bufferPool.fetchPage(pageId)` 获取一个页面，修改它，并看到它被自动刷回磁盘文件。
- **持久化检查**：程序关闭后，磁盘上的 `.db` 文件大小是 4KB 的整数倍。

### 3. 验收步骤 (可见结果)

1. 编写一个测试类，向 `BufferPool` 请求 `Page 0`。
2. 在 `Page 0` 的第 100 字节写入字符串 `"Hello MiniDB"`。
3. 调用 `bufferPool.unpinPage(0, true)`（标记为脏页）。
4. 强制关闭程序，查看磁盘文件。
5. **验证**：再次运行程序，读取 `Page 0`，能正确输出 `"Hello MiniDB"`。

---

## Stage 2：数据行与简单的扫描器 (Tuple & Scan)

![image-20260121184359446](/Users/hunkyhsu/Library/Application Support/typora-user-images/image-20260121184359446.png)

**目标**：在 4KB 的 Page 上构建一套数据结构，使其能存储变长的行记录（Tuple），并支持增删改查。

### 1. 核心概念：Slotted Page (槽位页)

我们不能简单地把数据一行行挨着写，因为如果删除了中间的一行，会留下空洞。**Slotted Page** 是解决这个问题的标准方案。

#### 1.1 页面布局 (Page Layout)

一个 4KB 的 `TablePage` 结构如下：

```
+----------------+-----------------------------------------------------------+
|  Header (页头)  |  Free Space (空闲空间)  <-- 动态伸缩 -->  Tuples (数据区)   |
+----------------+-----------------------------------------------------------+
^                ^                       ^                  ^                ^
0                24 bytes               Gap End            Gap Start        4096
```

1. **Header (固定大小)**:
   - `PageId` (4B)
   - `PrevPageId` (4B): 双向链表，指向前一个数据页。
   - `NextPageId` (4B): 双向链表，指向后一个数据页。
   - `FreeSpacePointer` (4B): 指向数据区空闲空间的**起始偏移量**（即 Gap End 的位置）。初始值是 4096。
   - `TupleCount` (4B): 当前页有多少个 Tuple（包括被标记删除的）。
2. **Slots (槽位区, 从 Header 之后开始)**:
   - 这是一个数组，每个 Slot 占用 4 字节（或 8 字节）。
   - **Slot 结构**: `[Offset (2B), Size (2B)]`。
   - `Slot[0]` 指向第一个 Tuple 的偏移量和长度，`Slot[1]` 指向第二个，以此类推。
   - 数组从前往后增长。
3. **Tuples (数据区, 从 Page 末尾向前增长)**:
   - 实际的数据内容。
   - 插入 Tuple 1 时，数据写在 `[4096 - len1, 4096]`。
   - 插入 Tuple 2 时，数据写在 `[4096 - len1 - len2, 4096 - len1]`。
   - 数组从后往前增长。

**关键点**：当 `Slots` 数组的末尾 和 `Tuples` 数据的头部 相遇时，页面就满了。

### 2. 数据行 (Tuple) 的格式

Tuple 是数据库中的一行记录。它在内存中是一个对象，但在磁盘上就是一串字节。

#### 2.1 序列化格式 (Serialization)

为了简单起见，我们假设表结构目前只支持固定模式（例如：`int id, varchar name`）。但为了通用性，Tuple 自身应该只是一堆字节。

**存储格式**:

```
[Data Bytes ...]
```

- MiniDB 的 Stage 2 暂时不需要复杂的 Schema 解析（Catalog），我们可以先硬编码测试，或者简单地存储纯字节流。
- **注意**：在 Stage 2，我们主要关注 **Tuple 的存储管理**，而不是 Tuple 内部字段的解析。

### 3. 核心组件设计

#### 3.1 TablePage (extends Page)

- **职责**：将通用的 `Page` (byte[]) 包装成有结构的 `TablePage`。
- **核心方法**:
  - `insertTuple(Tuple tuple)`: 尝试在当前页插入。如果空间不够，返回 false。
  - `getTuple(int slotId)`: 根据槽位号读取 Tuple。
  - `markDelete(int slotId)`: 标记删除（不立即回收空间）。

#### 3.2 TableHeap (堆表)

- **职责**：管理一张表的所有 Page（通过双向链表连接）。
- **逻辑**:
  - `TableHeap` 持有 `FirstPageId`。
  - **插入 (Insert)**: 从第一页开始找，看哪页有空位。如果都满了，调用 `BufferPool.newPage()` 创建新页，并更新链表指针 (`NextPageId`)。
  - **扫描 (Iterator)**: 提供一个迭代器，从 FirstPage 的 Slot 0 开始，遍历到 LastPage 的最后一个 Slot。

### 4. 阶段二开发步骤 (Step-by-Step)

1. **Tuple 类**: 实现数据的序列化/反序列化（简单的包装类）。
2. **TablePage 类**: 实现 Slotted Page 的布局逻辑（这是最复杂的位操作部分）。
3. **TableHeap 类**: 实现跨页面的管理。
4. **测试验证**: 插入 1000 条数据，遍历验证。



---

## 阶段三：事务、日志与并发 (ACID & Concurrency)

**目标：** 实现 WAL 日志和 LockManager（这是最难的）。

**核心逻辑：** 引入事务 ID，确保修改前先写日志。

### 1. 任务指标
- 实现 LogManager：顺序追加日志到 .log 文件
- 实现 LockManager：实现一个 Map，记录哪个 TxID 锁住了哪个 Rid (Record ID)
- 实现 RecoveryManager：模拟程序崩溃，重启后根据日志恢复 BufferPool
- **【新增】实现 TransactionManager（事务管理器）：**
  - 分配唯一的事务 ID (TxID)
  - 管理事务状态：Running、Committed、Aborted
  - 实现事务的 Begin、Commit、Rollback 接口
- **【新增】实现 Checkpoint 机制：**
  - 实现 `CheckpointManager`：定期将 BufferPool 中的脏页刷盘
  - 在日志中记录 Checkpoint 位置（LSN）
  - 恢复时从最近的 Checkpoint 开始，避免回放所有日志
- **【新增】实现隔离级别（READ COMMITTED）：**
  - 读操作获取共享锁（S-Lock），读完立即释放
  - 写操作获取排他锁（X-Lock），事务提交时释放
  - 实现死锁检测：超时机制或依赖图检测

### 2. 验收标准
- **原子性：** 事务 Rollback 后，内存和磁盘的数据必须变回原样
- **并发性：** 两个线程同时改两行不同的数据，互不干扰；改同一行，必须排队

### 3. 验收步骤（可见结果）
1. 开启事务 A，修改 ID=1 的数据
2. 在事务 A 提交前，开启事务 B 修改 ID=1
3. **验证：** 观察日志，事务 B 必须处于等待状态（Block）
4. 杀掉进程，删除 .db 文件中未提交的部分，重启
5. **验证：** 数据恢复到修改前的状态

---

## 阶段三点五：查询执行引擎 (Execution Engine)

**目标：** 实现火山模型执行器框架，支持 SQL 语句的执行。

**核心逻辑：** 将 SQL 语句转换为执行计划树，通过迭代器模式执行查询。

### 1. 任务指标
- **实现 Executor 接口（火山模型）：**
  - 定义 `Executor` 接口：`init()`、`next()`、`close()` 方法
  - 每次调用 `next()` 返回一条 Tuple，无结果时返回 null
- **实现基础执行器：**
  - `SeqScanExecutor`：全表顺序扫描
  - `IndexScanExecutor`：通过索引扫描（使用 B+Tree）
  - `FilterExecutor`：WHERE 条件过滤
  - `ProjectionExecutor`：SELECT 列投影（选择指定列）
- **实现修改操作执行器：**
  - `InsertExecutor`：执行 INSERT 语句
  - `UpdateExecutor`：执行 UPDATE 语句
  - `DeleteExecutor`：执行 DELETE 语句
- **实现 Planner（查询计划器）：**
  - 将解析后的 SQL（JSqlParser 输出）转换为执行计划树
  - 简单的优化：如果 WHERE 条件包含主键，使用 IndexScan 而非 SeqScan

### 2. 验收标准
- **查询执行：** 能够执行 `SELECT * FROM user WHERE id > 10;`，并正确过滤结果
- **修改执行：** 能够执行 `UPDATE user SET name = 'new' WHERE id = 1;`
- **删除执行：** 能够执行 `DELETE FROM user WHERE id = 5;`
- **执行器组合：** 能够构建执行器树，例如：Projection -> Filter -> SeqScan

### 3. 验收步骤（可见结果）
1. 插入 10 条测试数据：`INSERT INTO user VALUES (1, 'a'), (2, 'b'), ..., (10, 'j');`
2. 执行查询：`SELECT id, name FROM user WHERE id > 5;`
3. **验证：** 返回 5 条记录（id = 6, 7, 8, 9, 10）
4. 执行更新：`UPDATE user SET name = 'updated' WHERE id = 3;`
5. **验证：** 查询 id=3 的记录，name 字段已更新为 'updated'
6. 执行删除：`DELETE FROM user WHERE id = 7;`
7. **验证：** 查询所有记录，id=7 的记录已被删除

---

## 阶段四：SQL 接口与网络门户 (SQL & Network)

**目标：** 让用户通过网络点菜。

**核心逻辑：** 集成 Netty 和 JSqlParser，将 SQL 映射为阶段二的 Executor。

### 1. 任务指标
- 实现 NettyServer：监听 8888 端口
- 集成 JSqlParser：解析 INSERT、SELECT、UPDATE、DELETE
- **【完善】扩展 Catalog 功能：**
  - 不仅记录表的起始页，还要管理完整的 Schema 信息
  - 支持 `CREATE TABLE` 和 `DROP TABLE` 语句
  - 元数据持久化到系统表（`minidb_tables` 和 `minidb_columns`）
- **【新增】实现通信协议（Protocol Handler）：**
  - 定义请求格式：`[SQL长度(4字节)][SQL字符串]`
  - 定义响应格式：`[状态码(1字节)][行数(4字节)][Schema信息][行数据]`
  - 实现结果集序列化：将 Tuple 列表格式化为字节流
  - 支持错误码返回（语法错误、执行错误等）
- **【新增】实现 Result Formatter（结果格式化器）：**
  - 将查询结果格式化为表格形式（类似 MySQL 客户端）
  - 支持列对齐、边框绘制
  - 显示查询耗时和影响行数

### 2. 验收标准
- **交互性：** 使用 Telnet 或命令行客户端连接，输入 SQL 得到结果
- **DDL 支持：** 能够执行 `CREATE TABLE` 创建表，`DROP TABLE` 删除表
- **完整的 CRUD：** 支持 INSERT、SELECT、UPDATE、DELETE 四种操作
- **错误处理：** 语法错误或执行错误能返回清晰的错误信息

### 3. 验收步骤（可见结果）
1. 启动 MiniDBApplication
2. 打开终端执行：`telnet localhost 8888`
3. 创建表：`CREATE TABLE user (id INT PRIMARY KEY, name VARCHAR(50));`
4. **验证：** 返回 "Table created successfully"
5. 插入数据：`INSERT INTO user VALUES (1, 'arch');`
6. **验证：** 返回 "1 row inserted"
7. 查询数据：`SELECT * FROM user;`
8. **验证：** 终端屏幕弹出格式化的数据表格：
   ```
   +----+------+
   | id | name |
   +----+------+
   |  1 | arch |
   +----+------+
   1 row in set (0.05 sec)
   ```
9. 更新数据：`UPDATE user SET name = 'newname' WHERE id = 1;`
10. **验证：** 返回 "1 row updated"
11. 删除数据：`DELETE FROM user WHERE id = 1;`
12. **验证：** 返回 "1 row deleted"
13. 删除表：`DROP TABLE user;`
14. **验证：** 返回 "Table dropped successfully"

---

## 附录：MVP 功能检查清单

### ✅ 必须实现的核心功能

#### 存储引擎层
- [x] Page 结构定义（4KB）
- [x] DiskManager（磁盘读写）
- [x] BufferPoolManager（LRU 缓存）
- [x] 类型系统（Type、Value、Schema）

#### 数据访问层
- [x] Tuple（行对象）
- [x] Slotted Page（槽页结构）
- [x] TableHeap（表堆管理）
- [x] SeqScan（顺序扫描）
- [x] B+Tree 主键索引
- [x] IndexScan（索引扫描）
- [x] Catalog（系统目录）
- [x] 元数据持久化

#### 事务层
- [x] TransactionManager（事务管理）
- [x] LogManager（WAL 日志）
- [x] LockManager（锁管理）
- [x] RecoveryManager（崩溃恢复）
- [x] CheckpointManager（检查点）
- [x] READ COMMITTED 隔离级别
- [x] 死锁检测

#### 执行引擎层
- [x] Executor 接口（火山模型）
- [x] SeqScanExecutor
- [x] IndexScanExecutor
- [x] FilterExecutor
- [x] ProjectionExecutor
- [x] InsertExecutor
- [x] UpdateExecutor
- [x] DeleteExecutor
- [x] Planner（查询计划器）

#### SQL 接口层
- [x] NettyServer（网络服务）
- [x] JSqlParser（SQL 解析）
- [x] Protocol Handler（通信协议）
- [x] Result Formatter（结果格式化）
- [x] CREATE TABLE / DROP TABLE
- [x] INSERT / SELECT / UPDATE / DELETE

### ❌ MVP 暂不实现的高级特性

- [ ] JOIN 操作（INNER JOIN、LEFT JOIN 等）
- [ ] 子查询（Subquery）
- [ ] 聚合函数（COUNT、SUM、AVG、GROUP BY、HAVING）
- [ ] 排序（ORDER BY）
- [ ] 分页（LIMIT、OFFSET）
- [ ] 视图（VIEW）
- [ ] 存储过程（Stored Procedure）
- [ ] 触发器（Trigger）
- [ ] 外键约束（Foreign Key）
- [ ] MVCC（多版本并发控制）
- [ ] 查询优化器（基于代价的优化）
- [ ] 统计信息收集
- [ ] 二级索引（非主键索引）
- [ ] 全文索引
- [ ] 分布式支持

---

## 开发建议

### 1. 开发顺序
建议严格按照阶段顺序开发，每个阶段完成后进行充分测试再进入下一阶段。

### 2. 测试策略
- **单元测试：** 每个核心类都编写单元测试，覆盖率 > 80%
- **集成测试：** 每个阶段完成后进行端到端测试
- **性能测试：** 最后阶段进行压力测试，目标 1000+ TPS

### 3. 代码组织
参考 `architecture.md` 中的包结构建议，保持清晰的分层架构。

### 4. 文档记录
- 记录关键设计决策
- 记录遇到的问题和解决方案
- 记录性能瓶颈和优化思路

### 5. 参考资料
- CMU 15-445 Database Systems 课程
- 《数据库系统实现》（Database System Implementation）
- 《事务处理：概念与技术》（Transaction Processing）
- SQLite、Derby、BusTub 等开源项目

---

## 总结

完成以上所有阶段后，你将拥有一个功能完整的 MVP 级别的 MySQL-like 数据库系统，具备：

✅ **完整的存储引擎**：支持数据持久化和缓存管理
✅ **ACID 事务支持**：保证数据一致性和可靠性
✅ **并发控制**：支持多线程并发访问
✅ **崩溃恢复**：程序崩溃后能自动恢复数据
✅ **SQL 接口**：支持通过网络执行 SQL 语句
✅ **索引加速**：通过 B+Tree 索引提升查询性能

这是一个可以实际运行、演示和扩展的数据库系统，为后续添加更多高级特性打下了坚实的基础。