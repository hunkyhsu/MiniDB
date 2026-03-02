package com.hunkyhsu.minidb.type;

import java.nio.ByteBuffer;

public interface Type {
    TypeId getTypeId();

    int getSerializedSize(Value value);

    void serialize(Value value, ByteBuffer buffer);

    Value deserialize(ByteBuffer buffer);

    boolean isCompatible(Type other);
}
