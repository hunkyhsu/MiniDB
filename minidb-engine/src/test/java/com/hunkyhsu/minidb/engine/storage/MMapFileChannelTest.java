package com.hunkyhsu.minidb.engine.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 10:14
 */
class MMapFileChannelTest {
    private static final String TEST_PATH = "test_data/minidb_test.dat";
    private static final int FILE_SIZE = 10 * 1024 * 1024; // 10MB
    private MMapFileChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        Files.deleteIfExists(Paths.get(TEST_PATH));
        channel = new MMapFileChannel(TEST_PATH, FILE_SIZE);
    }
    @AfterEach
    void tearDown() throws Exception {
        Files.deleteIfExists(Paths.get(TEST_PATH));
        Files.deleteIfExists(Paths.get("test_data"));
    }
    @Test
    @DisplayName("Single Thread RW")
    void testSingleThreadRW(){
        long pointer = TuplePointer.pack(0, 100);
        long expectedXmin = 999L;
        byte[] expectedPayload = "Hello World".getBytes(StandardCharsets.UTF_8);
        channel.writeTuple(pointer, expectedXmin, expectedPayload);
        assertEquals(expectedXmin, channel.readXmin(pointer), "Xmin should be equal to " + expectedXmin);
        assertEquals(expectedPayload.length, channel.readPayloadLength(pointer), "Length of payload should be equal to " + expectedPayload.length);
        assertArrayEquals(expectedPayload, channel.readPayload(pointer));
    }
    @Test
    @DisplayName("Multi Thread RW")
    void testMultiThreadRW() throws InterruptedException {
        int numThreads = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                try {
                    long pointer = TuplePointer.pack(threadIndex, 0);
                    long xmin = 1000L + threadIndex;
                    String msg = "Current Message From Thread " + threadIndex;
                    byte[] payload = msg.getBytes(StandardCharsets.UTF_8);
                    channel.writeTuple(pointer, xmin, payload);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();
        for (int i = 0; i < numThreads; i++) {
            long pointer = TuplePointer.pack(i, 0);
            long expectedXmin = 1000L + i;
            String expectedMsg = "Current Message From Thread " + i;
            byte[] expectedPayload = expectedMsg.getBytes(StandardCharsets.UTF_8);
            assertEquals(expectedXmin, channel.readXmin(pointer), "Xmin should be equal to " + expectedXmin);
            assertEquals(expectedPayload.length, channel.readPayloadLength(pointer), "Length of payload should be equal to " + expectedPayload.length);
            assertArrayEquals(expectedPayload, channel.readPayload(pointer));
        }
    }
}
