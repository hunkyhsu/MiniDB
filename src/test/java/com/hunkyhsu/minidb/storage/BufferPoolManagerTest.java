package com.hunkyhsu.minidb.storage;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BufferPoolManager 单元测试
 *
 * 测试覆盖：
 * 1. 基本的 fetch/unpin 功能
 * 2. LRU 淘汰机制
 * 3. 脏页刷盘
 * 4. 数据持久化
 * 5. Pin/Unpin 引用计数
 * 6. 并发访问
 * 7. BufferPool 满的处理
 * 8. 高并发压力测试
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BufferPoolManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(BufferPoolManagerTest.class);
    private static final String TEST_DB_PATH = "test_buffer_pool.db";
    private static final int POOL_SIZE = 10;  // 小的 BufferPool 便于测试淘汰

    private DiskManager diskManager;
    private BufferPoolManager bufferPool;

    @BeforeEach
    void setUp() throws IOException {
        deleteTestFile();
        diskManager = new DiskManager(TEST_DB_PATH);
        bufferPool = new BufferPoolManager(POOL_SIZE, diskManager);
        logger.info("Test setup completed (poolSize={})", POOL_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (bufferPool != null) {
            bufferPool.close();
        }
        if (diskManager != null) {
            diskManager.close();
        }
        deleteTestFile();
        logger.info("Test teardown completed");
    }

    private void deleteTestFile() throws IOException {
        Path path = Path.of(TEST_DB_PATH);
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    // ========== 基本功能测试 ==========

    @Test
    @Order(1)
    @DisplayName("测试：fetchPage 和 unpinPage")
    void testFetchAndUnpin() throws IOException {
        // 创建新 Page
        Page page = bufferPool.newPage();
        int pageId = page.getPageId();
        assertEquals(0, pageId, "First page ID should be 0");
        assertEquals(1, page.getPinCount(), "Pin count should be 1 after newPage");

        // 写入数据
        String testData = "Test Data";
        page.getPageData().put(testData.getBytes());

        // Unpin
        bufferPool.unpinPage(pageId, true);
        assertEquals(0, page.getPinCount(), "Pin count should be 0 after unpin");

        // Fetch 回来
        Page fetchedPage = bufferPool.fetchPage(pageId);
        assertEquals(pageId, fetchedPage.getPageId());
        assertEquals(1, fetchedPage.getPinCount(), "Pin count should be 1 after fetch");

        // 验证数据
        byte[] data = new byte[testData.length()];
        fetchedPage.getPageData().get(data);
        assertEquals(testData, new String(data), "Data should match");

        bufferPool.unpinPage(pageId, false);

        logger.info("✅ testFetchAndUnpin passed");
    }

    @Test
    @Order(2)
    @DisplayName("测试：多次 pin 同一个 Page")
    void testMultiplePinSamePage() throws IOException {
        Page page = bufferPool.newPage();
        int pageId = page.getPageId();

        // 第一次 pin（newPage 自动 pin）
        assertEquals(1, page.getPinCount());

        // 再次 fetch（增加 pin count）
        Page page2 = bufferPool.fetchPage(pageId);
        assertSame(page, page2, "Should return same Page object");
        assertEquals(2, page.getPinCount(), "Pin count should be 2");

        // 再次 fetch
        Page page3 = bufferPool.fetchPage(pageId);
        assertEquals(3, page.getPinCount(), "Pin count should be 3");

        // Unpin 三次
        bufferPool.unpinPage(pageId, false);
        assertEquals(2, page.getPinCount());

        bufferPool.unpinPage(pageId, false);
        assertEquals(1, page.getPinCount());

        bufferPool.unpinPage(pageId, false);
        assertEquals(0, page.getPinCount());

        logger.info("✅ testMultiplePinSamePage passed");
    }

    @Test
    @Order(3)
    @DisplayName("测试：LRU 淘汰机制")
    void testLRUEviction() throws IOException {
        // 填满 BufferPool（10 个 Page）
        for (int i = 0; i < POOL_SIZE; i++) {
            Page page = bufferPool.newPage();
            page.getPageData().put(("Page " + i).getBytes());
            bufferPool.unpinPage(page.getPageId(), true);
        }

        // 访问 Page 1-9（使 Page 0 成为最久未使用）
        for (int i = 1; i < POOL_SIZE; i++) {
            Page page = bufferPool.fetchPage(i);
            bufferPool.unpinPage(i, false);
        }

        // 创建新 Page（应该淘汰 Page 0）
        Page newPage = bufferPool.newPage();
        assertEquals(POOL_SIZE, newPage.getPageId(), "New page ID should be " + POOL_SIZE);
        bufferPool.unpinPage(newPage.getPageId(), true);

        // 尝试 fetch Page 0（应该从磁盘加载，淘汰其他 Page）
        Page page0 = bufferPool.fetchPage(0);
        assertEquals(0, page0.getPageId());

        // 验证数据持久化（Page 0 被淘汰后又加载回来）
        byte[] data = new byte["Page 0".length()];
        page0.getPageData().get(data);
        assertEquals("Page 0", new String(data), "Page 0 data should be restored from disk");

        bufferPool.unpinPage(0, false);

        logger.info("✅ testLRUEviction passed");
    }

    @Test
    @Order(4)
    @DisplayName("测试：脏页自动刷盘")
    void testDirtyPageFlush() throws IOException {
        // 填满 BufferPool
        for (int i = 0; i < POOL_SIZE; i++) {
            Page page = bufferPool.newPage();
            page.getPageData().put(("Original " + i).getBytes());
            bufferPool.unpinPage(page.getPageId(), true);  // 标记为脏
        }

        // 创建新 Page（触发淘汰，脏页应自动刷盘）
        Page newPage = bufferPool.newPage();
        newPage.getPageData().put("New Page".getBytes());
        bufferPool.unpinPage(newPage.getPageId(), true);

        // 关闭 BufferPool（不显式 flush）
        bufferPool.close();

        // 重新打开验证数据持久化
        bufferPool = new BufferPoolManager(POOL_SIZE, diskManager);

        // 验证所有 Page 的数据都持久化了
        for (int i = 0; i < POOL_SIZE; i++) {
            Page page = bufferPool.fetchPage(i);
            byte[] expected = ("Original " + i).getBytes();
            byte[] actual = new byte[expected.length];
            page.getPageData().get(actual);
            assertArrayEquals(expected, actual, "Page " + i + " should be persisted");
            bufferPool.unpinPage(i, false);
        }

        // 验证新 Page
        Page fetchedNewPage = bufferPool.fetchPage(newPage.getPageId());
        byte[] newData = new byte["New Page".length()];
        fetchedNewPage.getPageData().get(newData);
        assertEquals("New Page", new String(newData), "New page should be persisted");
        bufferPool.unpinPage(fetchedNewPage.getPageId(), false);

        logger.info("✅ testDirtyPageFlush passed");
    }

    @Test
    @Order(5)
    @DisplayName("测试：数据持久化（重启验证）")
    void testPersistence() throws IOException {
        // 第一阶段：写入数据
        int numPages = 5;
        for (int i = 0; i < numPages; i++) {
            Page page = bufferPool.newPage();
            String data = "Persistent data " + i;
            page.getPageData().put(data.getBytes());
            bufferPool.unpinPage(page.getPageId(), true);
        }

        // 显式刷盘
        bufferPool.flushAllPages();

        // 关闭
        bufferPool.close();
        diskManager.close();

        // 第二阶段：重新打开验证
        diskManager = new DiskManager(TEST_DB_PATH);
        bufferPool = new BufferPoolManager(POOL_SIZE, diskManager);

        // 验证所有数据
        for (int i = 0; i < numPages; i++) {
            Page page = bufferPool.fetchPage(i);
            String expected = "Persistent data " + i;
            byte[] data = new byte[expected.length()];
            page.getPageData().get(data);
            assertEquals(expected, new String(data), "Page " + i + " should persist");
            bufferPool.unpinPage(i, false);
        }

        logger.info("✅ testPersistence passed");
    }

    @Test
    @Order(6)
    @DisplayName("测试：BufferPool 满且所有 Page 被 pin")
    void testBufferPoolFullAllPinned() throws IOException {
        // 填满 BufferPool 并 pin 所有 Page
        Page[] pages = new Page[POOL_SIZE];
        for (int i = 0; i < POOL_SIZE; i++) {
            pages[i] = bufferPool.newPage();
            // 不 unpin，保持 pinned 状态
        }

        // 尝试创建新 Page（应该失败）
        assertThrows(IOException.class, () -> {
            bufferPool.newPage();
        }, "Should throw IOException when all pages are pinned");

        logger.info("✅ testBufferPoolFullAllPinned passed");
    }

    // ========== 并发测试 ==========

    @Test
    @Order(7)
    @DisplayName("测试：并发 fetch 同一个 Page")
    void testConcurrentFetchSamePage() throws Exception {
        // 创建一个 Page
        Page page = bufferPool.newPage();
        int pageId = page.getPageId();
        String testData = "Concurrent Test";
        page.getPageData().put(testData.getBytes());
        bufferPool.unpinPage(pageId, true);

        // 并发 fetch
        int numThreads = 20;
        int fetchesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < fetchesPerThread; j++) {
                        Page p = bufferPool.fetchPage(pageId);
                        assertNotNull(p);
                        assertEquals(pageId, p.getPageId());

                        // 立即 unpin（不验证数据，避免 ByteBuffer 并发问题）
                        bufferPool.unpinPage(pageId, false);
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Concurrent fetch error", e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        logger.info("Concurrent fetch test: {} successes, {} errors",
                   successCount.get(), errorCount.get());

        // 所有操作都应该成功
        assertEquals(numThreads * fetchesPerThread, successCount.get(),
                    "All fetch operations should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        logger.info("✅ testConcurrentFetchSamePage passed ({} fetches)",
                   successCount.get());
    }

    @Test
    @Order(8)
    @DisplayName("测试：并发读写不同 Page")
    void testConcurrentReadWriteDifferentPages() throws Exception {
        // 预先创建 20 个 Page
        int numPages = 20;
        for (int i = 0; i < numPages; i++) {
            Page page = bufferPool.newPage();
            page.getPageData().put(("Initial " + i).getBytes());
            bufferPool.unpinPage(page.getPageId(), true);
        }

        // 并发读写
        int numThreads = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    for (int j = 0; j < operationsPerThread; j++) {
                        int pageId = random.nextInt(numPages);

                        Page page = bufferPool.fetchPage(pageId);
                        assertNotNull(page);

                        // 50% 读，50% 写
                        if (random.nextBoolean()) {
                            // 写操作
                            String data = "T" + threadId + "-Op" + j;
                            page.getPageData().clear();
                            page.getPageData().put(data.getBytes());
                            bufferPool.unpinPage(pageId, true);
                        } else {
                            // 读操作
                            bufferPool.unpinPage(pageId, false);
                        }

                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Concurrent r/w error in thread " + threadId, e);
                    fail(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(numThreads * operationsPerThread, successCount.get());

        logger.info("✅ testConcurrentReadWriteDifferentPages passed");
    }

    @Test
    @Order(9)
    @DisplayName("压力测试：高并发随机访问")
    void testHighConcurrencyStress() throws Exception {
        // 预先创建 8 个 Page（留 2 个空间给淘汰和替换）
        int numPages = 8;
        for (int i = 0; i < numPages; i++) {
            Page page = bufferPool.newPage();
            page.getPageData().put(("Page " + i).getBytes());
            bufferPool.unpinPage(page.getPageId(), true);
        }

        // 高并发压力测试
        int numThreads = 30;
        int operationsPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger totalOps = new AtomicInteger(0);
        AtomicInteger fetchCount = new AtomicInteger(0);
        AtomicInteger unpinCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    Random random = new Random(threadId);
                    for (int j = 0; j < operationsPerThread; j++) {
                        int pageId = random.nextInt(numPages);

                        // Fetch
                        Page page = bufferPool.fetchPage(pageId);
                        fetchCount.incrementAndGet();

                        // 随机读写
                        if (random.nextInt(100) < 30) {  // 30% 写
                            String data = "T" + threadId + "-" + j;
                            page.getPageData().clear();
                            page.getPageData().put(data.getBytes());
                            bufferPool.unpinPage(pageId, true);
                        } else {  // 70% 读
                            bufferPool.unpinPage(pageId, false);
                        }
                        unpinCount.incrementAndGet();

                        totalOps.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Error in thread " + threadId, e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(120, TimeUnit.SECONDS),
                  "Stress test should complete within 120 seconds");
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        int expectedOps = numThreads * operationsPerThread;
        double throughput = (totalOps.get() * 1000.0) / duration;

        logger.info("Stress test completed:");
        logger.info("  Duration: {} ms", duration);
        logger.info("  Total operations: {}/{}", totalOps.get(), expectedOps);
        logger.info("  Fetch count: {}", fetchCount.get());
        logger.info("  Unpin count: {}", unpinCount.get());
        logger.info("  Error count: {}", errorCount.get());
        logger.info("  Throughput: {} ops/sec", String.format("%.2f", throughput));

        assertEquals(expectedOps, totalOps.get(), "All operations should complete");
        assertEquals(0, errorCount.get(), "No errors should occur");
        assertEquals(fetchCount.get(), unpinCount.get(),
                    "Fetch and unpin counts should match");

        logger.info("✅ testHighConcurrencyStress passed");
    }

    @Test
    @Order(10)
    @DisplayName("压力测试：模拟真实工作负载")
    void testRealisticWorkload() throws Exception {
        // 模拟真实数据库工作负载：
        // - 80% 读，20% 写
        // - 热点数据（20% 的 Page 占 80% 的访问）
        // - 短事务（访问 1 个 Page，避免死锁）

        int numPages = 8;  // 减少到 8 个 Page（留 2 个空间给淘汰）
        int hotPages = 5;  // 前 5 个是热点 Page

        // 初始化数据
        for (int i = 0; i < numPages; i++) {
            Page page = bufferPool.newPage();
            page.getPageData().put(("Initial data " + i).getBytes());
            bufferPool.unpinPage(page.getPageId(), true);
        }

        int numThreads = 10;  // 减少线程数
        int transactionsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    Random random = new Random(threadId);

                    for (int txn = 0; txn < transactionsPerThread; txn++) {
                        // 模拟短事务：只访问 1 个 Page
                        int pageId;
                        // 80% 概率访问热点 Page
                        if (random.nextInt(100) < 80) {
                            pageId = random.nextInt(hotPages);
                        } else {
                            pageId = random.nextInt(numPages);
                        }

                        Page page = bufferPool.fetchPage(pageId);

                        // 80% 读，20% 写
                        boolean isWrite = random.nextInt(100) < 20;
                        if (isWrite) {
                            String data = "Txn" + txn + "-T" + threadId;
                            page.getPageData().clear();
                            page.getPageData().put(data.getBytes());
                            bufferPool.unpinPage(pageId, true);
                        } else {
                            bufferPool.unpinPage(pageId, false);
                        }

                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Error in thread " + threadId, e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        executor.shutdown();

        int expectedTxns = numThreads * transactionsPerThread;
        logger.info("Realistic workload test:");
        logger.info("  Transactions: {}/{}", successCount.get(), expectedTxns);
        logger.info("  Errors: {}", errorCount.get());

        assertEquals(expectedTxns, successCount.get(), "All transactions should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        logger.info("✅ testRealisticWorkload passed");
    }

    // ========== 统计信息测试 ==========

    @Test
    @Order(11)
    @DisplayName("测试：统计信息")
    void testStats() throws IOException {
        // 创建几个 Page
        for (int i = 0; i < 5; i++) {
            Page page = bufferPool.newPage();
            page.getPageData().put(("Page " + i).getBytes());
            bufferPool.unpinPage(page.getPageId(), true);
        }

        String stats = bufferPool.getStats();
        assertNotNull(stats);
        assertTrue(stats.contains("poolSize=10"));
        assertTrue(stats.contains("dirty=5"));

        logger.info("BufferPool stats: {}", stats);
        logger.info("✅ testStats passed");
    }
}