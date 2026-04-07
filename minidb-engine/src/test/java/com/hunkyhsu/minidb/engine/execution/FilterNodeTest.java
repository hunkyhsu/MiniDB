package com.hunkyhsu.minidb.engine.execution;

import com.hunkyhsu.minidb.engine.storage.AppendOnlyTableStore;
import com.hunkyhsu.minidb.engine.storage.MMapFileChannel;
import com.hunkyhsu.minidb.engine.storage.PageLayout;
import com.hunkyhsu.minidb.engine.storage.TuplePointer;
import com.hunkyhsu.minidb.engine.transaction.GlobalCommitLog;
import com.hunkyhsu.minidb.engine.transaction.TransactionContext;
import com.hunkyhsu.minidb.engine.transaction.TransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/5 21:13
 */
class FilterNodeTest {
    private static final String TEST_DB_PATH = "test_db/filter_test.dat";
    private static final int FILE_SIZE = 10 * 1024 * 1024;
    private MMapFileChannel channel;
    private AppendOnlyTableStore store;
    private TransactionManager txManager;

    @BeforeEach
    void setUp() throws Exception {
        Files.deleteIfExists(Paths.get(TEST_DB_PATH));
        channel = new MMapFileChannel(TEST_DB_PATH, FILE_SIZE);
        store = new AppendOnlyTableStore(channel);
        GlobalCommitLog clog = new GlobalCommitLog(10000);
        txManager = new TransactionManager(clog);
    }
    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(TEST_DB_PATH));
        Files.deleteIfExists(Paths.get("test_db"));
    }

    private byte[] serializeUser(int id, int age) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(id);
        buffer.putInt(age);
        return buffer.array();
    }

    @Test
    @DisplayName("End-to-end Filter Test")
    void endToEndFilterTest() {
        long tx1 = txManager.beginWriteTransaction();
        store.insertTuple(tx1, serializeUser(1, 15));
        txManager.commitTransaction(tx1);
        long tx2 = txManager.beginWriteTransaction();
        long tx2Ptr = store.insertTuple(tx2, serializeUser(2, 22));
        txManager.commitTransaction(tx2);
        long tx3 = txManager.beginWriteTransaction();
        store.insertTuple(tx3, serializeUser(3, 30));
        txManager.abortTransaction(tx3);
        long tx4 = txManager.beginWriteTransaction();
        long tx4Ptr = store.insertTuple(tx4, serializeUser(4, 18));
        txManager.commitTransaction(tx4);

        TransactionContext snapshot = txManager.beginReadSnapshot(9999);

        Predicate<Long> ageGT18 = pointer -> {
            int pageId = TuplePointer.getPageId(pointer);
            int pageOffset = TuplePointer.getOffset(pointer);
            int ageAbsOffset = pageId * PageLayout.PAGE_SIZE + pageOffset + PageLayout.HEADER_SIZE + Integer.BYTES;
            byte[] payload = channel.readPayload(pointer);
            int age = ByteBuffer.wrap(payload).getInt(4);
            return age >= 18;
        };

        SeqScanNode scanNode = new SeqScanNode(channel, store, snapshot);
        FilterNode filterNode = new FilterNode(scanNode, ageGT18);
        filterNode.open();
        List<Long> resultList = new ArrayList<>();
        long currentPtr;
        while ((currentPtr = filterNode.next()) != DbIterator.EOF) {
            resultList.add(currentPtr);
        }
        filterNode.close();

        assertEquals(2, resultList.size(), "Should be 2 results");
        assertEquals(tx2Ptr, resultList.get(0), "1st Element should be tx2 pointer");
        assertEquals(tx4Ptr, resultList.get(1), "2nd Element should be tx4 pointer");
    }
}
