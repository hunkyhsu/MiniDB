package com.hunkyhsu.minidb.tuple;

import com.hunkyhsu.minidb.storage.Page;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TablePageTest {

    private TablePage newTablePage(int pageId) {
        Page page = new Page();
        page.setPageId(pageId);
        TablePage tablePage = new TablePage(page);
        tablePage.init(pageId, -1);
        return tablePage;
    }

    @Test
    void initSetsHeaderFields() {
        TablePage tablePage = newTablePage(3);
        assertEquals(3, tablePage.getPageId());
        assertEquals(-1, tablePage.getPrevPageId());
        assertEquals(-1, tablePage.getNextPageId());
        assertEquals(0, tablePage.getTupleCount());
    }

    @Test
    void insertAndGetRoundTrip() {
        TablePage tablePage = newTablePage(1);
        byte[] data = "hello".getBytes();
        int slotId = tablePage.insertTuple(new Tuple(data));
        assertEquals(0, slotId);
        assertEquals(1, tablePage.getTupleCount());

        Tuple tuple = tablePage.getTuple(slotId);
        assertNotNull(tuple);
        assertEquals(1, tuple.getRecordId().getPageId());
        assertEquals(slotId, tuple.getRecordId().getSlotId());
        assertArrayEquals(data, tuple.getData());
    }

    @Test
    void updateTupleSmallerSucceeds() {
        TablePage tablePage = newTablePage(2);
        int slotId = tablePage.insertTuple(new Tuple("abcdef".getBytes()));

        Tuple newTuple = new Tuple("abc".getBytes());
        assertTrue(tablePage.updateTuple(newTuple, slotId));

        Tuple updated = tablePage.getTuple(slotId);
        assertNotNull(updated);
        assertArrayEquals("abc".getBytes(), updated.getData());
    }

    @Test
    void updateTupleLargerFailsAndKeepsData() {
        TablePage tablePage = newTablePage(2);
        int slotId = tablePage.insertTuple(new Tuple("abc".getBytes()));

        Tuple larger = new Tuple("abcd".getBytes());
        assertFalse(tablePage.updateTuple(larger, slotId));

        Tuple updated = tablePage.getTuple(slotId);
        assertNotNull(updated);
        assertArrayEquals("abc".getBytes(), updated.getData());
    }

    @Test
    void markDeletedHidesTuple() {
        TablePage tablePage = newTablePage(4);
        int slotId = tablePage.insertTuple(new Tuple("row".getBytes()));
        assertTrue(tablePage.markDeleted(slotId));
        assertNull(tablePage.getTuple(slotId));
        assertFalse(tablePage.updateTuple(new Tuple("new".getBytes()), slotId));
    }

    @Test
    void deleteTupleUsesRecordId() {
        TablePage tablePage = newTablePage(5);
        int slotId = tablePage.insertTuple(new Tuple("row".getBytes()));

        Tuple tuple = tablePage.getTuple(slotId);
        assertNotNull(tuple);
        assertEquals(slotId, tablePage.deleteTuple(tuple));
        assertNull(tablePage.getTuple(slotId));
    }

    @Test
    void slotsAreNotReusedAfterDelete() {
        TablePage tablePage = newTablePage(6);
        int slotId0 = tablePage.insertTuple(new Tuple("a".getBytes()));
        assertTrue(tablePage.markDeleted(slotId0));

        int slotId1 = tablePage.insertTuple(new Tuple("b".getBytes()));
        assertEquals(1, slotId1);
        assertEquals(2, tablePage.getTupleCount());
        assertNull(tablePage.getTuple(slotId0));
        assertNotNull(tablePage.getTuple(slotId1));
    }

    @Test
    void insertTooLargeTupleThrows() {
        TablePage tablePage = newTablePage(7);
        byte[] tooLarge = new byte[Page.PAGE_SIZE];
        assertThrows(IllegalArgumentException.class, () -> tablePage.insertTuple(new Tuple(tooLarge)));
    }
}
