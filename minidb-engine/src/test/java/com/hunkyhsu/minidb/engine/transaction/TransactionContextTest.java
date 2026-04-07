package com.hunkyhsu.minidb.engine.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 22:33
 */
class TransactionContextTest {
    private GlobalCommitLog clog;
    private TransactionContext snapshot;

    @BeforeEach
    void setUp() {
        clog = new GlobalCommitLog(100);
        clog.setStatus(10, GlobalCommitLog.COMMITTED);
        clog.setStatus(11, GlobalCommitLog.ABORTED);
        clog.setStatus(12, GlobalCommitLog.IN_PROGRESS);
        clog.setStatus(13, GlobalCommitLog.COMMITTED);
        clog.setStatus(14, GlobalCommitLog.IN_PROGRESS);

        long[] activeTxns = {12, 14};
        snapshot = new TransactionContext(15, 12, 15, activeTxns, clog);
    }

    @Test
    @DisplayName("Self Transaction Test")
    void selfTransactionTest() {
        assertTrue(snapshot.isVisible(15), "Transaction must be visible to itself");
    }

    @Test
    @DisplayName("Future Transaction Test")
    void futureTransactionTest() {
        assertFalse(snapshot.isVisible(16), " Future Transaction must be invisible");
        assertFalse(snapshot.isVisible(999), "Future Transaction must be invisible");
    }
    @Test
    @DisplayName("Historical Transaction Test")
    void historicalTransactionTest() {
        assertTrue(snapshot.isVisible(10), "Historical Committed Transaction should be visible");
        assertFalse(snapshot.isVisible(11), "Historical Aborted Transaction should be invisible");
    }
    @Test
    @DisplayName("Active Transaction Test")
    void activeTransactionTest() {
        assertFalse(snapshot.isVisible(12), "Active Transaction should be invisible");
        assertTrue(snapshot.isVisible(13), "Active But Committed Transaction should be visible");
        assertFalse(snapshot.isVisible(14), "Active Transaction should be invisible");
    }
}
