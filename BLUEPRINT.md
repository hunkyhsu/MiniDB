1. 项目初始化结构 (Project Initialization)
   为了在工程物理层面上强制隔离控制面（Spring Boot）与数据面（Pure Java），建议采用 Maven 或 Gradle 的多模块 (Multi-module) 架构构建项目。

minidb-server (控制面模块)：依赖 Spring Boot Web 框架，负责提供 RESTful API，接收 GET /api/select 和 POST /api/insert 请求，充当网络与序列化边界。

minidb-engine (数据面模块)：纯 Java 实现，零第三方依赖。负责所有的物理存储、事务并发与查询执行。

minidb-engine 的核心包结构（参考了 SimpleDB 等经典数据库项目的最佳实践  以及 QuestDB 的高性能模块划分 ）：

org.minidb.access: 负责磁盘文件的堆外内存映射（Memory-Mapped Files）与页面缓冲池。

org.minidb.tx: 负责基于 MVCC 的事务上下文与全局提交日志（CLOG）。

org.minidb.execution: 火山模型计算引擎，包含各种算子节点。

org.minidb.catalog: 负责表元数据的单例管理 。

2. 核心架构接口蓝图 (Architecture Blueprint)
   在 minidb-engine 中，你需要定义以下核心组件的接口与契约，以跑通 MVP 数据流：

A. 物理与访问层 (Physical & Access Layer)
为了实现零 GC，底层决不能频繁实例化 Tuple 对象，而是要传递基于内存映射的“游标”。

MMapFileChannel: 封装 Java NIO 的 FileChannel.map()。它将磁盘数据文件直接映射为操作系统的 Page Cache，供引擎以原生的字节进行读写 。

TuplePointer: 一个极简的数值型结构（例如封装一个 long 类型，前 32 位是页 ID，后 32 位是偏移量），所有算子之间只传递这个指针。

B. 事务层 (Transaction Layer)
摒弃传统的锁管理器，转而使用无锁并发数据结构。

GlobalCommitLog (CLOG): 使用原生的 long 数组或 BitSet 维护一个紧凑的全局事务状态机，利用按位运算（Bitwise operations）以无锁方式记录事务的提交状态 。

TransactionContext: 事务上下文快照。在查询启动时瞬间冻结，包含当前事务 ID (T_current)、活跃事务集合（位图）以及高低水位线。它向外暴露一个核心方法：boolean isVisible(long xmin)，用于判断一条记录是否对当前查询可见。

C. 执行层 (Execution Layer)
遵循火山模型（Volcano Model）的标准迭代器模式。

DbIterator: 所有算子的顶级接口，定义了 open(), next(), close() 方法 。

SeqScanNode: 表扫描算子。它在初始化时会被注入 TransactionContext。当它调用底层的 MMapFileChannel 读取到 TuplePointer 时，会直接从字节流头部提取出写入该记录的事务 ID（xmin），并调用上下文的 isVisible(xmin)。只有可见的指针才会被 next() 返回 。

FilterNode: 谓词过滤算子。它接收来自 SeqScanNode 的指针，同样在堆外内存上直接进行字节比对（例如判断整型是否匹配），满足条件的指针才会继续向上层流动。

3. 数据流转落地策略 (MVP Workflow Execution)
   INSERT 操作路径（追加写）：
   当控制面收到写入请求，minidb-engine 生成一个新的事务 ID（xmin）。引擎通过无锁的原子累加操作（例如 AtomicInteger.getAndAdd()）在 MMapFileChannel 的尾部获取一段独占的连续字节区间。接着，将 xmin 与数据体序列化写入这段内存中，最后利用内存屏障更新 GlobalCommitLog 标记该事务已提交。全程无锁，极度轻量。

SELECT 操作路径（延迟物化）：
查询请求到达后，生成只读的 TransactionContext 快照。火山模型的执行树开始流转，SeqScanNode 和 FilterNode 在堆外内存中高速遍历和筛选出符合条件的 TuplePointer 集合。直到这些指针即将跨越引擎边界返回给 Spring Boot 控制器时，才最后一次性地（Late Materialization）将所需的堆外字节拷贝并实例化为普通的 Java DTO 对象并转换为 JSON 返回。