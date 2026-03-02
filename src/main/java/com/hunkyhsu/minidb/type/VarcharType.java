package com.hunkyhsu.minidb.type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VarcharType implements Type {
    private final int maxLength;

    public VarcharType(int maxLength) {
        if (maxLength <= 0) {
            throw new IllegalArgumentException("maxLength must be positive");
        }
        this.maxLength = maxLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    @Override
    public TypeId getTypeId() {
        return TypeId.VARCHAR;
    }

    // coding : [length of varchar: int][bytes...]
    @Override
    public int getSerializedSize(Value value) {
        String text = value.asVarchar();
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxLength) {
            throw new IllegalArgumentException("VARCHAR length exceeds maxLength: " + bytes.length);
        }
        return Integer.BYTES + bytes.length;
    }

    @Override
    public void serialize(Value value, ByteBuffer buffer) {
        String text = value.asVarchar();
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxLength) {
            throw new IllegalArgumentException("VARCHAR length exceeds maxLength: " + bytes.length);
        }
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    @Override
    public Value deserialize(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || length > maxLength) {
            throw new IllegalArgumentException("Invalid VARCHAR length: " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return Value.ofVarchar(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public boolean isCompatible(Type other) {
        if (!(other instanceof VarcharType)) {
            return false;
        }
        VarcharType otherType = (VarcharType) other;
        return maxLength == otherType.maxLength;
    }
}
