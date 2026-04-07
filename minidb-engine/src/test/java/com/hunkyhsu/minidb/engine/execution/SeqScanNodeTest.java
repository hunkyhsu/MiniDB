package com.hunkyhsu.minidb.engine.execution;

import com.hunkyhsu.minidb.engine.storage.AppendOnlyTableStore;
import com.hunkyhsu.minidb.engine.storage.MMapFileChannel;
import com.hunkyhsu.minidb.engine.storage.PageLayout;
import com.hunkyhsu.minidb.engine.transaction.GlobalCommitLog;
import com.hunkyhsu.minidb.engine.transaction.TransactionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/5 21:13
 */
class SeqScanNodeTest {
    private static final String TEST_DB_PATH = "test_db/seqscan_test.dat";
    private static final int FILE_SIZE = 10 * 1024 * 1024;
    private MMapFileChannel channel;
    private AppendOnlyTableStore store;
    private GlobalCommitLog clog;
    @BeforeEach
    void setUp() throws IOException {
        Files.deleteIfExists(Paths.get(TEST_DB_PATH));
        channel = new MMapFileChannel(TEST_DB_PATH, FILE_SIZE);
        store = new AppendOnlyTableStore(channel);
        clog = new GlobalCommitLog(1000);
    }
    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(TEST_DB_PATH));
        Files.deleteIfExists(Paths.get("test_db"));
    }
    @Test
    @DisplayName("End-to-end Seq Scan Test")
    void endToEndSeqScanTest() {
        long p1 = store.insertTuple(1001, new byte[100]);
        clog.setStatus(1001, GlobalCommitLog.COMMITTED);
        long p2 = store.insertTuple(1002, new byte[100]);
        clog.setStatus(1002, GlobalCommitLog.ABORTED);
        long p3 = store.insertTuple(1003, new byte[PageLayout.PAGE_SIZE - 150]);
        clog.setStatus(1003, GlobalCommitLog.COMMITTED);
        long p4 = store.insertTuple(1004, new byte[100]);
        clog.setStatus(1004, GlobalCommitLog.IN_PROGRESS);

        long[] activeTxns = {1004};
        TransactionContext snapshot = new TransactionContext(9999, 1004, 1005, activeTxns, clog);
        SeqScanNode node = new SeqScanNode(channel, store, snapshot);
        node.open();

        long expectedTx1 = node.next();
        assertEquals(p1, expectedTx1, "Read visible tx1");
        long expectedTx3 = node.next();
        assertEquals(p3, expectedTx3, "Read visible tx3");
        long expectedEOF = node.next();
        assertEquals(DbIterator.EOF, expectedEOF, "Return EOF when Read invisible tx4");
        node.close();

    }
}
