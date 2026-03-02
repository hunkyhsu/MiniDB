package com.hunkyhsu.minidb.tuple;

import com.hunkyhsu.minidb.schema.Column;
import com.hunkyhsu.minidb.schema.Schema;
import com.hunkyhsu.minidb.type.Type;
import com.hunkyhsu.minidb.type.TypeId;
import com.hunkyhsu.minidb.type.Value;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Tuple {
    private RecordId recordId;
    private final Schema schema;
    private final Value[] values;
    private byte[] data;

    public Tuple(Schema schema, Value[] values) {
        this(schema, values, null);
    }

    public Tuple(Schema schema, Value[] values, RecordId recordId) {
        if (schema == null) {
            throw new IllegalArgumentException("schema is null");
        }
        if (values == null) {
            throw new IllegalArgumentException("values is null");
        }
        validateValues(schema, values);
        this.schema = schema;
        this.values = Arrays.copyOf(values, values.length);
        this.recordId = recordId;
    }

    public static Tuple fromBytes(Schema schema, byte[] data, RecordId recordId) {
        if (schema == null) {
            throw new IllegalArgumentException("schema is null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data is null");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int columnCount = schema.getColumnCount();
        int nullBitmapSize = getNullBitmapSize(columnCount);
        byte[] nullBitmap = new byte[nullBitmapSize];
        buffer.get(nullBitmap);

        int fixedSize = 0;
        int varlenCount = 0;
        for (int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            if (isVarlenType(type)) {
                varlenCount++;
            } else {
                fixedSize += getFixedSize(type);
            }
        }
        int offsetTableStart = nullBitmapSize + fixedSize;
        int varlenDataStart = offsetTableStart + varlenCount * Integer.BYTES;
        int[] varlenEndOffsets = new int[varlenCount];
        buffer.position(offsetTableStart);
        for (int i = 0; i < varlenCount; i++) {
            varlenEndOffsets[i] = buffer.getInt();
        }

        Value[] values = new Value[columnCount];
        int fixedPos = nullBitmapSize;
        int varlenIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            boolean isNull = isNullBitSet(nullBitmap, i);
            if (isVarlenType(type)) {
                if (isNull) {
                    values[i] = Value.nullValue(type.getTypeId());
                } else {
                    int startOffset = varlenIndex == 0 ? 0 : varlenEndOffsets[varlenIndex - 1];
                    buffer.position(varlenDataStart + startOffset);
                    values[i] = type.deserialize(buffer);
                }
                varlenIndex++;
            } else {
                if (isNull) {
                    values[i] = Value.nullValue(type.getTypeId());
                    fixedPos += getFixedSize(type);
                } else {
                    buffer.position(fixedPos);
                    values[i] = type.deserialize(buffer);
                    fixedPos = buffer.position();
                }
            }
        }
        Tuple tuple = new Tuple(schema, values, recordId);
        tuple.data = Arrays.copyOf(data, data.length);
        return tuple;
    }

    private static void validateValues(Schema schema, Value[] values) {
        if (values.length != schema.getColumnCount()) {
            throw new IllegalArgumentException("value count does not match schema");
        }
        for (int i = 0; i < values.length; i++) {
            Column column = schema.getColumn(i);
            Value value = values[i];
            if (value == null || value.isNull()) {
                if (!column.isNullable()) {
                    throw new IllegalArgumentException("Column is not nullable: " + column.getName());
                }
                if (value != null && value.getTypeId() != column.getType().getTypeId()) {
                    throw new IllegalArgumentException("Null value type mismatch for column: " + column.getName());
                }
                continue;
            }
            if (value.getTypeId() != column.getType().getTypeId()) {
                throw new IllegalArgumentException("Value type mismatch for column: " + column.getName());
            }
        }
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }

    public Schema getSchema() {
        return schema;
    }

    public Value getValue(int index) {
        return values[index];
    }

    public Value getValue(String columnName) {
        return values[schema.getColumnIndex(columnName)];
    }

    public Value[] getValues() {
        return Arrays.copyOf(values, values.length);
    }

    public byte[] getData() {
        if (data == null) {
            data = serialize();
        }
        return data;
    }

    public int getSize() {
        return getData().length;
    }

    // Layout: [null bitmap][fixed-size][varlen offset table][varlen data].
    private byte[] serialize() {
        int columnCount = schema.getColumnCount();
        int nullBitmapSize = getNullBitmapSize(columnCount);
        byte[] nullBitmap = new byte[nullBitmapSize];

        int fixedSize = 0;
        int varlenCount = 0;
        int varlenDataSize = 0;
        for (int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            Value value = values[i];
            Type type = column.getType();
            boolean isNull = value == null || value.isNull();
            if (isNull) {
                setNullBit(nullBitmap, i);
            }
            if (isVarlenType(type)) {
                varlenCount++;
                if (!isNull) {
                    varlenDataSize += type.getSerializedSize(value);
                }
            } else {
                fixedSize += getFixedSize(type);
            }
        }
        int offsetTableSize = varlenCount * Integer.BYTES;
        int totalSize = nullBitmapSize + fixedSize + offsetTableSize + varlenDataSize;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.put(nullBitmap);
        buffer.position(nullBitmapSize);
        for (int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            if (isVarlenType(type)) {
                continue;
            }
            Value value = values[i];
            if (value == null || value.isNull()) {
                buffer.position(buffer.position() + getFixedSize(type));
                continue;
            }
            type.serialize(value, buffer);
        }

        int offsetTableStart = nullBitmapSize + fixedSize;
        buffer.position(offsetTableStart);
        int varlenOffset = 0;
        for (int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            if (!isVarlenType(type)) {
                continue;
            }
            Value value = values[i];
            if (value != null && !value.isNull()) {
                varlenOffset += type.getSerializedSize(value);
            }
            buffer.putInt(varlenOffset);
        }

        int varlenDataStart = offsetTableStart + offsetTableSize;
        buffer.position(varlenDataStart);
        for (int i = 0; i < columnCount; i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            if (!isVarlenType(type)) {
                continue;
            }
            Value value = values[i];
            if (value == null || value.isNull()) {
                continue;
            }
            type.serialize(value, buffer);
        }
        return buffer.array();
    }

    private static int getNullBitmapSize(int columnCount) {
        return (columnCount + 7) / 8;
    }

    private static boolean isNullBitSet(byte[] nullBitmap, int columnIndex) {
        int byteIndex = columnIndex / 8;
        int bitIndex = columnIndex % 8;
        return (nullBitmap[byteIndex] & (1 << bitIndex)) != 0;
    }

    private static void setNullBit(byte[] nullBitmap, int columnIndex) {
        int byteIndex = columnIndex / 8;
        int bitIndex = columnIndex % 8;
        nullBitmap[byteIndex] = (byte) (nullBitmap[byteIndex] | (1 << bitIndex));
    }

    private static boolean isVarlenType(Type type) {
        TypeId typeId = type.getTypeId();
        if (typeId == TypeId.INT) {
            return false;
        }
        if (typeId == TypeId.VARCHAR) {
            return true;
        }
        throw new IllegalArgumentException("Unknown type: " + typeId);
    }

    private static int getFixedSize(Type type) {
        TypeId typeId = type.getTypeId();
        if (typeId == TypeId.INT) {
            return Integer.BYTES;
        }
        throw new IllegalArgumentException("Variable-length type has no fixed size: " + typeId);
    }
}
