package com.hunkyhsu.minidb.engine.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/5 20:48
 */
class TransactionManagerTest {
    private GlobalCommitLog clog;
    private TransactionManager txManager;

    @BeforeEach
    void setUp() {
        clog = new GlobalCommitLog(100000);
        txManager = new TransactionManager(clog);
    }
    @Test
    @DisplayName("Basic Life Cycle Test")
    void basicLifeCycleTest() {
        long tx1 = txManager.beginWriteTransaction();
        long tx2 = txManager.beginWriteTransaction();
        txManager.commitTransaction(tx1);
        long tx3 = txManager.beginWriteTransaction();
        TransactionContext snapshot = txManager.beginReadSnapshot(9999);
        assertTrue(snapshot.isVisible(tx1), "tx1 should be visible");
        assertFalse(snapshot.isVisible(tx2), "tx2 should be invisible");
        assertFalse(snapshot.isVisible(tx3), "tx3 should be invisible");
        assertFalse(snapshot.isVisible(1003), "tx3 should be invisible");
    }
    @Test
    @DisplayName("Mutil Thread Test")
    void mutilThreadTest() {
        int writerCount = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        CountDownLatch latch = new CountDownLatch(writerCount);
        AtomicLong lastCommittedTxn = new AtomicLong(-1);
        for (int i = 0; i < writerCount; i++) {
            executorService.execute(() -> {
                try {
                    long xmin = txManager.beginWriteTransaction();
                    Thread.sleep((long) (Math.random() * 5));
                    txManager.commitTransaction(xmin);
                    lastCommittedTxn.set(xmin);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TransactionContext snapshot = txManager.beginReadSnapshot(8888);
        long safelyCommitted = lastCommittedTxn.get();
        latch.countDown();
        executorService.shutdown();
        if (safelyCommitted != -1 && safelyCommitted < 1000 + writerCount) {
            snapshot.isVisible(safelyCommitted);
        }
        TransactionContext finalSnapshot = txManager.beginReadSnapshot(9999);
        assertTrue(finalSnapshot.isVisible(1000), "First Transaction should be visible");
        assertTrue(finalSnapshot.isVisible(1000 + writerCount - 1), "Last Transaction should be visible");
    }
}
