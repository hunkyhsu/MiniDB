package com.hunkyhsu.minidb.engine.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 21:44
 */
class GlobalCommitLogTest {
    private GlobalCommitLog clog;

    @BeforeEach
    void setUp() {
        clog = new GlobalCommitLog(100000);
    }
    @Test
    @DisplayName("Single Thread Test")
    void testSingleThread() throws InterruptedException {
        assertEquals(GlobalCommitLog.IN_PROGRESS, clog.getStatus(0));
        assertEquals(GlobalCommitLog.IN_PROGRESS, clog.getStatus(1));
        assertEquals(GlobalCommitLog.IN_PROGRESS, clog.getStatus(2));
        assertEquals(GlobalCommitLog.IN_PROGRESS, clog.getStatus(3));

        clog.setStatus(1, GlobalCommitLog.COMMITTED);
        clog.setStatus(2, GlobalCommitLog.ABORTED);

        assertEquals(GlobalCommitLog.IN_PROGRESS, clog.getStatus(0));
        assertEquals(GlobalCommitLog.COMMITTED, clog.getStatus(1));
        assertEquals(GlobalCommitLog.ABORTED, clog.getStatus(2));
        assertEquals(GlobalCommitLog.IN_PROGRESS, clog.getStatus(3));
    }

    @Test
    @DisplayName("Boundary Test")
    void testBoundary() {
        clog.setStatus(31, GlobalCommitLog.COMMITTED);
        clog.setStatus(32, GlobalCommitLog.ABORTED);

        assertEquals(GlobalCommitLog.COMMITTED, clog.getStatus(31));
        assertEquals(GlobalCommitLog.ABORTED, clog.getStatus(32));
    }

    @Test
    @DisplayName("Mutil Thread Test")
    void testMutilThread() throws InterruptedException {
        int numThreads = 32;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final int xmin = i;
            executor.submit(() -> {
                try {
                    if (xmin % 2 == 0) {
                        clog.setStatus(xmin, GlobalCommitLog.COMMITTED);
                    } else {
                        clog.setStatus(xmin, GlobalCommitLog.ABORTED);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();

        for (int i = 0; i < numThreads; i++) {
            if (i % 2 == 0) {
                assertEquals(GlobalCommitLog.COMMITTED, clog.getStatus(i), "Wrong Status: Transaction " + i);
            } else {
                assertEquals(GlobalCommitLog.ABORTED, clog.getStatus(i), "Wrong Status: Transaction " + i);
            }
        }
    }
}
