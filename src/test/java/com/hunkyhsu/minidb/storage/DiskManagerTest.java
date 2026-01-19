package com.hunkyhsu.minidb.storage;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DiskManager 单元测试
 *
 * 测试覆盖：
 * 1. 基本读写功能
 * 2. 数据持久化
 * 3. Page 分配和释放
 * 4. 并发读写
 * 5. 异常处理
 * 6. 性能压力测试
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DiskManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(DiskManagerTest.class);
    private static final String TEST_DB_PATH = "test_disk_manager.db";

    private DiskManager diskManager;

    @BeforeEach
    void setUp() throws IOException {
        // 删除旧测试文件
        deleteTestFile();

        // 创建新的 DiskManager
        diskManager = new DiskManager(TEST_DB_PATH);
        logger.info("Test setup completed");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (diskManager != null) {
            diskManager.close();
        }

        // 清理测试文件
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
    @DisplayName("测试：分配新 Page")
    void testAllocatePage() throws IOException {
        // 分配第一个 Page
        int pageId = diskManager.allocatePage();
        assertEquals(0, pageId, "First page ID should be 0");

        // 分配第二个 Page
        int pageId2 = diskManager.allocatePage();
        assertEquals(1, pageId2, "Second page ID should be 1");

        // 验证文件大小
        long expectedSize = 2 * Page.PAGE_SIZE;
        assertEquals(expectedSize, diskManager.getFileSize(),
                    "File size should be 2 * PAGE_SIZE");

        logger.info("✅ testAllocatePage passed");
    }

    @Test
    @Order(2)
    @DisplayName("测试：写入和读取 Page")
    void testWriteAndReadPage() throws IOException {
        // 分配一个 Page
        int pageId = diskManager.allocatePage();

        // 准备测试数据
        Page writePage = new Page();
        writePage.setPageId(pageId);
        byte[] testData = "Hello MiniDB Test".getBytes();
        writePage.getPageData().put(testData);

        // 写入磁盘
        diskManager.writePage(pageId, writePage);

        // 读取磁盘
        Page readPage = new Page();
        diskManager.readPage(pageId, readPage);

        // 验证数据
        byte[] readData = new byte[testData.length];
        readPage.getPageData().get(readData);
        assertArrayEquals(testData, readData, "Read data should match written data");

        logger.info("✅ testWriteAndReadPage passed");
    }

    @Test
    @Order(3)
    @DisplayName("测试：反复读写同一个 Page")
    void testRepeatedReadWrite() throws IOException {
        int pageId = diskManager.allocatePage();
        int iterations = 100;

        for (int i = 0; i < iterations; i++) {
            // 写入数据
            Page writePage = new Page();
            writePage.setPageId(pageId);
            String testString = "Iteration " + i;
            writePage.getPageData().put(testString.getBytes());
            diskManager.writePage(pageId, writePage);

            // 立即读取验证
            Page readPage = new Page();
            diskManager.readPage(pageId, readPage);
            byte[] readData = new byte[testString.length()];
            readPage.getPageData().get(readData);
            assertEquals(testString, new String(readData),
                        "Data mismatch at iteration " + i);
        }

        logger.info("✅ testRepeatedReadWrite passed ({} iterations)", iterations);
    }

    @Test
    @Order(4)
    @DisplayName("测试：数据持久化（重启验证）")
    void testPersistence() throws IOException {
        // 第一阶段：写入数据
        int pageId = diskManager.allocatePage();
        Page writePage = new Page();
        writePage.setPageId(pageId);
        String persistentData = "This data must survive restart";
        writePage.getPageData().put(persistentData.getBytes());
        diskManager.writePage(pageId, writePage);

        // 关闭 DiskManager（模拟程序关闭）
        diskManager.close();

        // 第二阶段：重新打开并验证
        diskManager = new DiskManager(TEST_DB_PATH);
        assertEquals(1, diskManager.getNumPages(), "Should have 1 page after restart");

        Page readPage = new Page();
        diskManager.readPage(pageId, readPage);
        byte[] readData = new byte[persistentData.length()];
        readPage.getPageData().get(readData);
        assertEquals(persistentData, new String(readData),
                    "Data should persist after restart");

        logger.info("✅ testPersistence passed");
    }

    @Test
    @Order(5)
    @DisplayName("测试：多个 Page 的数据隔离")
    void testMultiplePageIsolation() throws IOException {
        // 分配 10 个 Page
        int numPages = 10;
        int[] pageIds = new int[numPages];
        for (int i = 0; i < numPages; i++) {
            pageIds[i] = diskManager.allocatePage();
        }

        // 写入不同数据到每个 Page
        for (int i = 0; i < numPages; i++) {
            Page page = new Page();
            page.setPageId(pageIds[i]);
            String data = "Page " + i + " data";
            page.getPageData().put(data.getBytes());
            diskManager.writePage(pageIds[i], page);
        }

        // 验证每个 Page 的数据独立
        for (int i = 0; i < numPages; i++) {
            Page page = new Page();
            diskManager.readPage(pageIds[i], page);
            String expectedData = "Page " + i + " data";
            byte[] readData = new byte[expectedData.length()];
            page.getPageData().get(readData);
            assertEquals(expectedData, new String(readData),
                        "Page " + i + " data should be isolated");
        }

        logger.info("✅ testMultiplePageIsolation passed");
    }

    // ========== 异常处理测试 ==========

    @Test
    @Order(6)
    @DisplayName("测试：读取无效的 Page ID")
    void testReadInvalidPageId() {
        assertThrows(IllegalArgumentException.class, () -> {
            Page page = new Page();
            diskManager.readPage(-1, page);
        }, "Should throw exception for negative page ID");

        assertThrows(IllegalArgumentException.class, () -> {
            Page page = new Page();
            diskManager.readPage(999, page);
        }, "Should throw exception for non-existent page ID");

        logger.info("✅ testReadInvalidPageId passed");
    }

    @Test
    @Order(7)
    @DisplayName("测试：写入无效的 Page ID")
    void testWriteInvalidPageId() {
        assertThrows(IllegalArgumentException.class, () -> {
            Page page = new Page();
            diskManager.writePage(-1, page);
        }, "Should throw exception for negative page ID");

        assertThrows(IllegalArgumentException.class, () -> {
            Page page = new Page();
            diskManager.writePage(999, page);
        }, "Should throw exception for non-existent page ID");

        logger.info("✅ testWriteInvalidPageId passed");
    }

    // ========== 并发测试 ==========

    @Test
    @Order(8)
    @DisplayName("测试：并发读取同一个 Page")
    void testConcurrentRead() throws Exception {
        // 准备测试数据
        int pageId = diskManager.allocatePage();
        Page page = new Page();
        page.setPageId(pageId);
        String testData = "Concurrent Read Test Data";
        page.getPageData().put(testData.getBytes());
        diskManager.writePage(pageId, page);

        // 并发读取
        int numThreads = 10;
        int readsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < readsPerThread; j++) {
                        Page readPage = new Page();
                        diskManager.readPage(pageId, readPage);
                        byte[] data = new byte[testData.length()];
                        readPage.getPageData().get(data);
                        assertEquals(testData, new String(data));
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Concurrent read error", e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        executor.shutdown();

        assertEquals(numThreads * readsPerThread, successCount.get(),
                    "All reads should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        logger.info("✅ testConcurrentRead passed ({} threads, {} reads each)",
                   numThreads, readsPerThread);
    }

    @Test
    @Order(9)
    @DisplayName("测试：并发写入不同 Page")
    void testConcurrentWriteDifferentPages() throws Exception {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        ConcurrentHashMap<Integer, String> expectedData = new ConcurrentHashMap<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        // 每个线程写入自己的 Page
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    int pageId = diskManager.allocatePage();
                    String data = "Thread " + threadId + " data";
                    expectedData.put(pageId, data);

                    Page page = new Page();
                    page.setPageId(pageId);
                    page.getPageData().put(data.getBytes());
                    diskManager.writePage(pageId, page);

                } catch (Exception e) {
                    logger.error("Concurrent write error in thread " + threadId, e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        executor.shutdown();

        assertEquals(0, errorCount.get(), "No errors should occur");
        assertEquals(numThreads, diskManager.getNumPages(), "Should have " + numThreads + " pages");

        // 验证每个 Page 的数据
        for (var entry : expectedData.entrySet()) {
            Page page = new Page();
            diskManager.readPage(entry.getKey(), page);
            byte[] data = new byte[entry.getValue().length()];
            page.getPageData().get(data);
            assertEquals(entry.getValue(), new String(data),
                        "Page " + entry.getKey() + " data should match");
        }

        logger.info("✅ testConcurrentWriteDifferentPages passed ({} threads)", numThreads);
    }

    @Test
    @Order(10)
    @DisplayName("压力测试：高并发随机读写")
    void testHighConcurrencyRandomReadWrite() throws Exception {
        // 准备：分配 50 个 Page
        int numPages = 50;
        for (int i = 0; i < numPages; i++) {
            diskManager.allocatePage();
        }

        // 写入初始数据
        for (int i = 0; i < numPages; i++) {
            Page page = new Page();
            page.setPageId(i);
            page.getPageData().put(("Initial data " + i).getBytes());
            diskManager.writePage(i, page);
        }

        // 高并发随机读写
        int numThreads = 20;
        int operationsPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        Random random = new Random();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    Random threadRandom = new Random(threadId);
                    for (int j = 0; j < operationsPerThread; j++) {
                        int pageId = threadRandom.nextInt(numPages);

                        // 50% 读，50% 写
                        if (threadRandom.nextBoolean()) {
                            // 读操作
                            Page page = new Page();
                            diskManager.readPage(pageId, page);
                        } else {
                            // 写操作
                            Page page = new Page();
                            page.setPageId(pageId);
                            String data = "T" + threadId + "-Op" + j + "-P" + pageId;
                            page.getPageData().put(data.getBytes());
                            diskManager.writePage(pageId, page);
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

        assertTrue(latch.await(60, TimeUnit.SECONDS),
                  "Stress test should complete within 60 seconds");
        executor.shutdown();

        int totalOperations = numThreads * operationsPerThread;
        logger.info("Stress test completed: {} operations, {} success, {} errors",
                   totalOperations, successCount.get(), errorCount.get());

        assertEquals(totalOperations, successCount.get(), "All operations should succeed");
        assertEquals(0, errorCount.get(), "No errors should occur");

        logger.info("✅ testHighConcurrencyRandomReadWrite passed");
    }

    // ========== 性能测试 ==========

    @Test
    @Order(11)
    @DisplayName("性能测试：顺序写入性能")
    void testSequentialWritePerformance() throws IOException {
        int numPages = 1000;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numPages; i++) {
            int pageId = diskManager.allocatePage();
            Page page = new Page();
            page.setPageId(pageId);
            page.getPageData().put(("Page " + i).getBytes());
            diskManager.writePage(pageId, page);
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = (numPages * 1000.0) / duration;  // pages/sec

        logger.info("Sequential write: {} pages in {} ms ({} pages/sec)",
                   numPages, duration, String.format("%.2f", throughput));

        // 性能断言：至少 100 pages/sec
        assertTrue(throughput > 100,
                  "Sequential write throughput should be > 100 pages/sec");

        logger.info("✅ testSequentialWritePerformance passed");
    }

    @Test
    @Order(12)
    @DisplayName("性能测试：随机读取性能")
    void testRandomReadPerformance() throws IOException {
        // 准备：写入 1000 个 Page
        int numPages = 1000;
        for (int i = 0; i < numPages; i++) {
            int pageId = diskManager.allocatePage();
            Page page = new Page();
            page.setPageId(pageId);
            page.getPageData().put(("Page " + i).getBytes());
            diskManager.writePage(pageId, page);
        }

        // 随机读取测试
        Random random = new Random(42);  // 固定种子以便复现
        int numReads = 5000;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numReads; i++) {
            int pageId = random.nextInt(numPages);
            Page page = new Page();
            diskManager.readPage(pageId, page);
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = (numReads * 1000.0) / duration;  // reads/sec

        logger.info("Random read: {} reads in {} ms ({} reads/sec)",
                   numReads, duration, String.format("%.2f", throughput));

        // 性能断言：至少 500 reads/sec
        assertTrue(throughput > 500,
                  "Random read throughput should be > 500 reads/sec");

        logger.info("✅ testRandomReadPerformance passed");
    }
}