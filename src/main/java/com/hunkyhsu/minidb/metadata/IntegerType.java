package com.hunkyhsu.minidb.metadata;

import java.nio.ByteBuffer;

public class IntegerType implements Type {
    @Override
    public TypeId getTypeId() {
        return TypeId.INT;
    }

    @Override
    public int getSerializedSize(Value value) {
        return Integer.BYTES;
    }

    @Override
    public void serialize(Value value, ByteBuffer buffer) {
        buffer.putInt(value.asInt());
    }

    @Override
    public Value deserialize(ByteBuffer buffer) {
        return Value.ofInt(buffer.getInt());
    }

    @Override
    public boolean isCompatible(Type other) {
        return other instanceof IntegerType;
    }
}
