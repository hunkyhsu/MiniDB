package com.hunkyhsu.minidb.tuple;

import com.hunkyhsu.minidb.storage.BufferPoolManager;
import com.hunkyhsu.minidb.storage.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableHeapIntegrationTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Path dbPath;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        String fileName = testInfo.getTestMethod().orElseThrow().getName() + ".db";
        dbPath = tempDir.resolve(fileName);
        diskManager = new DiskManager(dbPath.toString());
        bufferPoolManager = new BufferPoolManager(2, diskManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (bufferPoolManager != null) {
            bufferPoolManager.close();
        }
        if (diskManager != null) {
            diskManager.close();
        }
        if (dbPath != null) {
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    void insertAcrossPagesAndIterate() {
        TableHeap tableHeap = new TableHeap(bufferPoolManager);
        byte[] dataA = bytesOfSize(3000, (byte) 'a');
        byte[] dataB = bytesOfSize(3000, (byte) 'b');

        RecordId ridA = tableHeap.insertTuple(new Tuple(dataA));
        RecordId ridB = tableHeap.insertTuple(new Tuple(dataB));

        assertEquals(0, ridA.getPageId());
        assertEquals(1, ridB.getPageId());

        Tuple fetchedA = tableHeap.getTuple(ridA);
        Tuple fetchedB = tableHeap.getTuple(ridB);
        assertArrayEquals(dataA, fetchedA.getData());
        assertArrayEquals(dataB, fetchedB.getData());

        List<Tuple> tuples = new ArrayList<>();
        TableIterator iterator = tableHeap.iterator();
        while (iterator.hasNext()) {
            tuples.add(iterator.next());
        }
        assertEquals(2, tuples.size());
        assertArrayEquals(dataA, tuples.get(0).getData());
        assertArrayEquals(dataB, tuples.get(1).getData());
    }

    @Test
    void markDeletedIsNotVisibleAndPersists() throws IOException {
        TableHeap tableHeap = new TableHeap(bufferPoolManager);
        RecordId ridA = tableHeap.insertTuple(new Tuple(bytesOfSize(100, (byte) 'a')));
        RecordId ridB = tableHeap.insertTuple(new Tuple(bytesOfSize(100, (byte) 'b')));
        RecordId ridC = tableHeap.insertTuple(new Tuple(bytesOfSize(100, (byte) 'c')));

        assertTrue(tableHeap.markDeleted(ridB));

        List<Tuple> tuples = new ArrayList<>();
        TableIterator iterator = tableHeap.iterator();
        while (iterator.hasNext()) {
            tuples.add(iterator.next());
        }
        assertEquals(2, tuples.size());
        assertArrayEquals(bytesOfSize(100, (byte) 'a'), tuples.get(0).getData());
        assertArrayEquals(bytesOfSize(100, (byte) 'c'), tuples.get(1).getData());

        int firstPageId = tableHeap.getFirstPageId();
        bufferPoolManager.close();
        diskManager.close();

        diskManager = new DiskManager(dbPath.toString());
        bufferPoolManager = new BufferPoolManager(2, diskManager);
        tableHeap = new TableHeap(bufferPoolManager, firstPageId);

        assertNotNull(tableHeap.getTuple(ridA));
        assertNull(tableHeap.getTuple(ridB));
        assertNotNull(tableHeap.getTuple(ridC));
    }

    @Test
    void updatePersistsAfterReopen() throws IOException {
        TableHeap tableHeap = new TableHeap(bufferPoolManager);
        RecordId rid = tableHeap.insertTuple(new Tuple(bytesOfSize(200, (byte) 'x')));
        Tuple updated = new Tuple(bytesOfSize(50, (byte) 'z'));
        assertTrue(tableHeap.updateTuple(rid, updated));

        int firstPageId = tableHeap.getFirstPageId();
        bufferPoolManager.close();
        diskManager.close();

        diskManager = new DiskManager(dbPath.toString());
        bufferPoolManager = new BufferPoolManager(2, diskManager);
        tableHeap = new TableHeap(bufferPoolManager, firstPageId);

        Tuple fetched = tableHeap.getTuple(rid);
        assertNotNull(fetched);
        assertArrayEquals(updated.getData(), fetched.getData());
    }

    @Test
    void deleteDoesNotReuseSlotWithinPage() {
        TableHeap tableHeap = new TableHeap(bufferPoolManager);
        RecordId ridA = tableHeap.insertTuple(new Tuple(bytesOfSize(50, (byte) 'm')));
        assertTrue(tableHeap.markDeleted(ridA));

        RecordId ridB = tableHeap.insertTuple(new Tuple(bytesOfSize(50, (byte) 'n')));
        assertEquals(ridA.getPageId(), ridB.getPageId());
        assertEquals(ridA.getSlotId() + 1, ridB.getSlotId());
    }

    private static byte[] bytesOfSize(int size, byte fill) {
        byte[] data = new byte[size];
        Arrays.fill(data, fill);
        return data;
    }
}
