package com.hunkyhsu.minidb.storage;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LRUReplacer 单元测试
 *
 * 测试覆盖：
 * 1. 基本的 pin/unpin/victim 功能
 * 2. LRU 淘汰顺序
 * 3. 边界条件
 * 4. 并发安全
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LRUReplacerTest {

    private static final Logger logger = LoggerFactory.getLogger(LRUReplacerTest.class);

    private LRUReplacer replacer;

    @BeforeEach
    void setUp() {
        replacer = new LRUReplacer(10);
        logger.info("LRU Replacer initialized with capacity 10");
    }

    // ========== 基本功能测试 ==========

    @Test
    @Order(1)
    @DisplayName("测试：基本的 unpin 和 victim")
    void testBasicUnpinAndVictim() {
        // 初始状态：没有可淘汰的 Frame
        assertEquals(-1, replacer.victim(), "Should return -1 when empty");
        assertEquals(0, replacer.size());

        // Unpin Frame 0, 1, 2
        replacer.unpin(0);
        replacer.unpin(1);
        replacer.unpin(2);
        assertEquals(3, replacer.size());

        // Victim 应该返回最久未使用的（Frame 0）
        assertEquals(0, replacer.victim());
        assertEquals(2, replacer.size());

        // 继续 victim
        assertEquals(1, replacer.victim());
        assertEquals(2, replacer.victim());

        // 现在应该为空
        assertEquals(-1, replacer.victim());
        assertEquals(0, replacer.size());

        logger.info("✅ testBasicUnpinAndVictim passed");
    }

    @Test
    @Order(2)
    @DisplayName("测试：pin 操作")
    void testPin() {
        // Unpin 三个 Frame
        replacer.unpin(0);
        replacer.unpin(1);
        replacer.unpin(2);
        assertEquals(3, replacer.size());

        // Pin Frame 1（从 LRU 列表移除）
        replacer.pin(1);
        assertEquals(2, replacer.size());

        // Victim 应该跳过 Frame 1
        assertEquals(0, replacer.victim());
        assertEquals(2, replacer.victim());

        // Frame 1 不在 LRU 列表中
        assertEquals(-1, replacer.victim());

        logger.info("✅ testPin passed");
    }

    @Test
    @Order(3)
    @DisplayName("测试：LRU 淘汰顺序")
    void testLRUOrder() {
        // Unpin 顺序：0, 1, 2, 3, 4
        for (int i = 0; i < 5; i++) {
            replacer.unpin(i);
        }

        // 访问 Frame 0（移到末尾）
        replacer.pin(0);
        replacer.unpin(0);

        // 访问 Frame 2（移到末尾）
        replacer.pin(2);
        replacer.unpin(2);

        // 现在顺序应该是：1, 3, 4, 0, 2
        assertEquals(1, replacer.victim());
        assertEquals(3, replacer.victim());
        assertEquals(4, replacer.victim());
        assertEquals(0, replacer.victim());
        assertEquals(2, replacer.victim());

        logger.info("✅ test LRU Order passed");
    }

    @Test
    @Order(4)
    @DisplayName("测试：重复 unpin 同一个 Frame")
    void testRepeatedUnpin() {
        replacer.unpin(0);
        assertEquals(1, replacer.size());

        // 重复 unpin（应该覆盖，不增加 size）
        replacer.unpin(0);
        assertEquals(1, replacer.size());

        // Victim 应该只返回一个
        assertEquals(0, replacer.victim());
        assertEquals(-1, replacer.victim());

        logger.info("✅ testRepeatedUnpin passed");
    }

    @Test
    @Order(5)
    @DisplayName("测试：pin 不存在的 Frame")
    void testPinNonExistentFrame() {
        replacer.unpin(0);
        replacer.unpin(1);

        // Pin 一个不在 LRU 列表中的 Frame（应该不报错）
        assertDoesNotThrow(() -> replacer.pin(999));

        // Size 不应该变化
        assertEquals(2, replacer.size());

        logger.info("✅ testPinNonExistentFrame passed");
    }


    // ========== 边界条件测试 ==========

    @Test
    @Order(6)
    @DisplayName("测试：容量限制")
    void testCapacity() {
        // Unpin 超过容量的 Frame
        for (int i = 0; i < 20; i++) {
            replacer.unpin(i);
        }

        // 所有 Frame 都应该在 LRU 列表中
        assertEquals(20, replacer.size());

        // 依次 victim
        for (int i = 0; i < 20; i++) {
            assertEquals(i, replacer.victim());
        }

        assertEquals(-1, replacer.victim());

        logger.info("✅ testCapacity passed");
    }

    @Test
    @Order(7)
    @DisplayName("测试：空 Replacer 操作")
    void testEmptyReplacer() {
        assertEquals(-1, replacer.victim());
        assertEquals(0, replacer.size());

        // Pin 空 Replacer（不应报错）
        assertDoesNotThrow(() -> replacer.pin(0));

        logger.info("✅ testEmptyReplacer passed");
    }

    // ========== 并发测试 ==========

    @Test
    @Order(8)
    @DisplayName("测试：并发 unpin")
    void testConcurrentUnpin() throws Exception {
        int numThreads = 10;
        int unpinsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < unpinsPerThread; j++) {
                        int frameId = threadId * unpinsPerThread + j;
                        replacer.unpin(frameId);
                    }
                } catch (Exception e) {
                    logger.error("Error in thread " + threadId, e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(0, errorCount.get(), "No errors should occur");
        assertEquals(numThreads * unpinsPerThread, replacer.size());

        logger.info("✅ testConcurrentUnpin passed ({} threads)", numThreads);
    }

    @Test
    @Order(9)
    @DisplayName("测试：并发 victim")
    void testConcurrentVictim() throws Exception {
        // 准备：unpin 1000 个 Frame
        int numFrames = 1000;
        for (int i = 0; i < numFrames; i++) {
            replacer.unpin(i);
        }

        // 并发 victim
        int numThreads = 10;
        int victimsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        Set<Integer> victimFrames = new HashSet<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < victimsPerThread; j++) {
                        int victim = replacer.victim();
                        if (victim != -1) {
                            synchronized (victimFrames) {
                                assertFalse(victimFrames.contains(victim),
                                           "Frame " + victim + " was already victimized");
                                victimFrames.add(victim);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error in victim thread", e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(0, errorCount.get(), "No errors should occur");
        assertEquals(numThreads * victimsPerThread, victimFrames.size(),
                    "All victims should be unique");

        logger.info("✅ testConcurrentVictim passed ({} victims)", victimFrames.size());
    }

    @Test
    @Order(10)
    @DisplayName("压力测试：并发混合操作")
    void testConcurrentMixedOperations() throws Exception {
        int numThreads = 20;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger unpinCount = new AtomicInteger(0);
        AtomicInteger pinCount = new AtomicInteger(0);
        AtomicInteger victimCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        int operation = (threadId * operationsPerThread + j) % 3;

                        switch (operation) {
                            case 0:  // unpin
                                int unpinFrame = threadId * 100 + j;
                                replacer.unpin(unpinFrame);
                                unpinCount.incrementAndGet();
                                break;

                            case 1:  // pin
                                int pinFrame = threadId * 100 + j;
                                replacer.pin(pinFrame);
                                pinCount.incrementAndGet();
                                break;

                            case 2:  // victim
                                replacer.victim();
                                victimCount.incrementAndGet();
                                break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error in thread " + threadId, e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        logger.info("Mixed operations completed:");
        logger.info("  Unpin: {}", unpinCount.get());
        logger.info("  Pin: {}", pinCount.get());
        logger.info("  Victim: {}", victimCount.get());
        logger.info("  Errors: {}", errorCount.get());
        logger.info("  Final size: {}", replacer.size());

        assertEquals(0, errorCount.get(), "No errors should occur");

        logger.info("✅ testConcurrentMixedOperations passed");
    }
}