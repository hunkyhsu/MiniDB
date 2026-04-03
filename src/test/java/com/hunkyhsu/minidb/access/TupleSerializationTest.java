package com.hunkyhsu.minidb.access;

import com.hunkyhsu.minidb.access.heap.Tuple;
import com.hunkyhsu.minidb.access.record.RecordId;
import com.hunkyhsu.minidb.metadata.schema.Column;
import com.hunkyhsu.minidb.metadata.schema.Schema;
import com.hunkyhsu.minidb.metadata.IntegerType;
import com.hunkyhsu.minidb.metadata.TypeId;
import com.hunkyhsu.minidb.metadata.Value;
import com.hunkyhsu.minidb.metadata.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TupleSerializationTest {

    @Test
    void serializeAndDeserializeRoundTrip() {
        Schema schema = new Schema(List.of(
                new Column("id", new IntegerType(), false),
                new Column("name", new VarcharType(20), false)
        ));
        Tuple tuple = new Tuple(schema, new Value[]{
                Value.ofInt(42),
                Value.ofVarchar("alice")
        }, new RecordId(1, 2));

        byte[] data = tuple.getData();
        Tuple decoded = Tuple.fromBytes(schema, data, new RecordId(1, 2));

        assertEquals(42, decoded.getValue(0).asInt());
        assertEquals("alice", decoded.getValue(1).asVarchar());
    }

    @Test
    void nullableColumnAllowsNullValue() {
        Schema schema = new Schema(List.of(
                new Column("name", new VarcharType(10), true)
        ));
        Tuple tuple = new Tuple(schema, new Value[]{
                Value.nullValue(TypeId.VARCHAR)
        });

        Tuple decoded = Tuple.fromBytes(schema, tuple.getData(), null);
        assertTrue(decoded.getValue(0).isNull());
    }

    @Test
    void varcharLengthLimitEnforced() {
        Schema schema = new Schema(List.of(
                new Column("name", new VarcharType(3), false)
        ));
        Tuple tuple = new Tuple(schema, new Value[]{
                Value.ofVarchar("abcd")
        });

        assertThrows(IllegalArgumentException.class, tuple::getData);
    }
}
