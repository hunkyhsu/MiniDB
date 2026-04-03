package com.hunkyhsu.minidb.access;

import com.hunkyhsu.minidb.access.heap.TablePage;
import com.hunkyhsu.minidb.access.heap.Tuple;
import com.hunkyhsu.minidb.metadata.schema.Column;
import com.hunkyhsu.minidb.metadata.schema.Schema;
import com.hunkyhsu.minidb.storage.Page;
import com.hunkyhsu.minidb.metadata.TypeId;
import com.hunkyhsu.minidb.metadata.Value;
import com.hunkyhsu.minidb.metadata.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TablePageTest {

    private TablePage newTablePage(int pageId) {
        Page page = new Page();
        page.setPageId(pageId);
        TablePage tablePage = new TablePage(page);
        tablePage.init(pageId, -1);
        return tablePage;
    }

    private static Schema varcharSchema(int maxLength, boolean nullable) {
        return new Schema(List.of(new Column("value", new VarcharType(maxLength), nullable)));
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
        Schema schema = varcharSchema(100, false);
        int slotId = tablePage.insertTuple(, tupleOf(schema, "hello"), );
        assertEquals(0, slotId);
        assertEquals(1, tablePage.getTupleCount());

        Tuple tuple = tablePage.getTuple(slotId, , schema);
        assertNotNull(tuple);
        assertEquals(1, tuple.getRecordId().getPageId());
        assertEquals(slotId, tuple.getRecordId().getSlotId());
        assertEquals("hello", tuple.getValue(0).asVarchar());
    }

    @Test
    void updateTupleSmallerSucceeds() {
        TablePage tablePage = newTablePage(2);
        Schema schema = varcharSchema(100, false);
        int slotId = tablePage.insertTuple(, tupleOf(schema, "abcdef"), );

        Tuple newTuple = tupleOf(schema, "abc");
        assertTrue(tablePage.updateTuple(newTuple, slotId));

        Tuple updated = tablePage.getTuple(slotId, , schema);
        assertNotNull(updated);
        assertEquals("abc", updated.getValue(0).asVarchar());
    }

    @Test
    void updateTupleLargerFailsAndKeepsData() {
        TablePage tablePage = newTablePage(2);
        Schema schema = varcharSchema(100, false);
        int slotId = tablePage.insertTuple(, tupleOf(schema, "abc"), );

        Tuple larger = tupleOf(schema, "abcd");
        assertFalse(tablePage.updateTuple(larger, slotId));

        Tuple updated = tablePage.getTuple(slotId, , schema);
        assertNotNull(updated);
        assertEquals("abc", updated.getValue(0).asVarchar());
    }

    @Test
    void markDeletedHidesTuple() {
        TablePage tablePage = newTablePage(4);
        Schema schema = varcharSchema(100, false);
        int slotId = tablePage.insertTuple(, tupleOf(schema, "row"), );
        assertTrue(tablePage.markDeleted(slotId));
        assertNull(tablePage.getTuple(slotId, , schema));
        assertFalse(tablePage.updateTuple(tupleOf(schema, "new"), slotId));
    }

    @Test
    void deleteTupleUsesRecordId() {
        TablePage tablePage = newTablePage(5);
        Schema schema = varcharSchema(100, false);
        int slotId = tablePage.insertTuple(, tupleOf(schema, "row"), );

        Tuple tuple = tablePage.getTuple(slotId, , schema);
        assertNotNull(tuple);
        assertEquals(slotId, tablePage.deleteTuple(tuple));
        assertNull(tablePage.getTuple(slotId, , schema));
    }

    @Test
    void slotsAreNotReusedAfterDelete() {
        TablePage tablePage = newTablePage(6);
        Schema schema = varcharSchema(100, false);
        int slotId0 = tablePage.insertTuple(, tupleOf(schema, "a"), );
        assertTrue(tablePage.markDeleted(slotId0));

        int slotId1 = tablePage.insertTuple(, tupleOf(schema, "b"), );
        assertEquals(1, slotId1);
        assertEquals(2, tablePage.getTupleCount());
        assertNull(tablePage.getTuple(slotId0, , schema));
        assertNotNull(tablePage.getTuple(slotId1, , schema));
    }

    @Test
    void insertTooLargeTupleThrows() {
        TablePage tablePage = newTablePage(7);
        Schema schema = varcharSchema(Page.PAGE_SIZE, false);
        String tooLarge = stringOf('x', Page.PAGE_SIZE);
        assertThrows(IllegalArgumentException.class, () -> tablePage.insertTuple(, tupleOf(schema, tooLarge), ));
    }
}
