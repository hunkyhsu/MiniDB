package com.hunkyhsu.minidb.engine.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 16:25
 */
class AppendOnlyTableStoreTest {
    private static final String TEST_DB_PATH = "test_db/minidb_store_test.dat";
    private static final int FILE_SIZE = 10 * 1024 * 1024;
    private MMapFileChannel channel;
    private AppendOnlyTableStore store;

    @BeforeEach
    void setUp() throws IOException {
        Files.deleteIfExists(Paths.get(TEST_DB_PATH));
        channel = new MMapFileChannel(TEST_DB_PATH, FILE_SIZE);
        store = new AppendOnlyTableStore(channel);
    }
    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(TEST_DB_PATH));
        Files.deleteIfExists(Paths.get("test_db"));
    }

    @Test
    @DisplayName("Oversized File Test")
    void oversizedFileTest() throws IOException {
        long xmin = 100L;
        byte[] payload = new byte[PageLayout.PAGE_SIZE + 1];
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            store.insertTuple(xmin, payload);
        });
        assertTrue(exception.getMessage().contains("Not Support Oversized Tuple"));
    }

    @Test
    @DisplayName("Concurrent Inserts And Page Jumps Test")
    void concurrentInsertsAndPageJumpsTest() throws InterruptedException {
        int numThreads = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        byte[] payloadTemplate = new byte[1000];
        for (int i = 0; i < numThreads; i++) {
            final long xmin = 1000L + i;
            executorService.submit(() -> {
                try {
                    store.insertTuple(xmin, payloadTemplate);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();
        int endOffset = store.getValidEndOffset();
        assertTrue(endOffset > 0, "End Offset should be greater than 0");

    }
}
