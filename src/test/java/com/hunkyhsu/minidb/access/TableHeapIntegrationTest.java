package com.hunkyhsu.minidb.access;

import com.hunkyhsu.minidb.access.heap.TableHeap;
import com.hunkyhsu.minidb.access.heap.TableIterator;
import com.hunkyhsu.minidb.access.heap.Tuple;
import com.hunkyhsu.minidb.access.record.RecordId;
import com.hunkyhsu.minidb.metadata.schema.Column;
import com.hunkyhsu.minidb.metadata.schema.Schema;
import com.hunkyhsu.minidb.storage.BufferPoolManager;
import com.hunkyhsu.minidb.storage.DiskManager;
import com.hunkyhsu.minidb.metadata.TypeId;
import com.hunkyhsu.minidb.metadata.Value;
import com.hunkyhsu.minidb.metadata.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableHeapIntegrationTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Path dbPath;
    private Schema schema;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        String fileName = testInfo.getTestMethod().orElseThrow().getName() + ".db";
        dbPath = tempDir.resolve(fileName);
        diskManager = new DiskManager(dbPath.toString());
        bufferPoolManager = new BufferPoolManager(2, diskManager);
        schema = new Schema(List.of(new Column("value", new VarcharType(4096), false)));
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
        TableHeap tableHeap = new TableHeap(bufferPoolManager, schema);
        String dataA = stringOf('a', 3000);
        String dataB = stringOf('b', 3000);

        RecordId ridA = tableHeap.insertTuple(tupleOf(schema, dataA));
        RecordId ridB = tableHeap.insertTuple(tupleOf(schema, dataB));

        assertEquals(0, ridA.getPageId());
        assertEquals(1, ridB.getPageId());

        Tuple fetchedA = tableHeap.getTuple(ridA);
        Tuple fetchedB = tableHeap.getTuple(ridB);
        assertEquals(dataA, fetchedA.getValue(0).asVarchar());
        assertEquals(dataB, fetchedB.getValue(0).asVarchar());

        List<Tuple> tuples = new ArrayList<>();
        TableIterator iterator = tableHeap.iterator();
        while (iterator.hasNext()) {
            tuples.add(iterator.next());
        }
        assertEquals(2, tuples.size());
        assertEquals(dataA, tuples.get(0).getValue(0).asVarchar());
        assertEquals(dataB, tuples.get(1).getValue(0).asVarchar());
    }

    @Test
    void markDeletedIsNotVisibleAndPersists() throws IOException {
        TableHeap tableHeap = new TableHeap(bufferPoolManager, schema);
        RecordId ridA = tableHeap.insertTuple(tupleOf(schema, stringOf('a', 100)));
        RecordId ridB = tableHeap.insertTuple(tupleOf(schema, stringOf('b', 100)));
        RecordId ridC = tableHeap.insertTuple(tupleOf(schema, stringOf('c', 100)));

        assertTrue(tableHeap.markDeleted(ridB));

        List<Tuple> tuples = new ArrayList<>();
        TableIterator iterator = tableHeap.iterator();
        while (iterator.hasNext()) {
            tuples.add(iterator.next());
        }
        assertEquals(2, tuples.size());
        assertEquals(stringOf('a', 100), tuples.get(0).getValue(0).asVarchar());
        assertEquals(stringOf('c', 100), tuples.get(1).getValue(0).asVarchar());

        int firstPageId = tableHeap.getFirstPageId();
        bufferPoolManager.close();
        diskManager.close();

        diskManager = new DiskManager(dbPath.toString());
        bufferPoolManager = new BufferPoolManager(2, diskManager);
        tableHeap = new TableHeap(bufferPoolManager, schema, firstPageId);

        assertNotNull(tableHeap.getTuple(ridA));
        assertNull(tableHeap.getTuple(ridB));
        assertNotNull(tableHeap.getTuple(ridC));
    }

    @Test
    void updatePersistsAfterReopen() throws IOException {
        TableHeap tableHeap = new TableHeap(bufferPoolManager, schema);
        RecordId rid = tableHeap.insertTuple(tupleOf(schema, stringOf('x', 200)));
        Tuple updated = tupleOf(schema, stringOf('z', 50));
        assertTrue(tableHeap.updateTuple(rid, updated));

        int firstPageId = tableHeap.getFirstPageId();
        bufferPoolManager.close();
        diskManager.close();

        diskManager = new DiskManager(dbPath.toString());
        bufferPoolManager = new BufferPoolManager(2, diskManager);
        tableHeap = new TableHeap(bufferPoolManager, schema, firstPageId);

        Tuple fetched = tableHeap.getTuple(rid);
        assertNotNull(fetched);
        assertEquals(updated.getValue(0).asVarchar(), fetched.getValue(0).asVarchar());
    }

    @Test
    void deleteDoesNotReuseSlotWithinPage() {
        TableHeap tableHeap = new TableHeap(bufferPoolManager, schema);
        RecordId ridA = tableHeap.insertTuple(tupleOf(schema, stringOf('m', 50)));
        assertTrue(tableHeap.markDeleted(ridA));

        RecordId ridB = tableHeap.insertTuple(tupleOf(schema, stringOf('n', 50)));
        assertEquals(ridA.getPageId(), ridB.getPageId());
        assertEquals(ridA.getSlotId() + 1, ridB.getSlotId());
    }

    private static Tuple tupleOf(Schema schema, String value) {
        Value v = value == null ? Value.nullValue(TypeId.VARCHAR) : Value.ofVarchar(value);
        return new Tuple(schema, new Value[]{v});
    }

    private static String stringOf(char ch, int size) {
        char[] data = new char[size];
        for (int i = 0; i < size; i++) {
            data[i] = ch;
        }
        return new String(data);
    }
}
