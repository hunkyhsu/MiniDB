package com.hunkyhsu.minidb.tuple;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@AllArgsConstructor
@Data
public class RecordId implements Serializable, Comparable<RecordId> {
    private static final long serialVersionUID = 1L;
    private final int pageId;
    private final int slotId;

    @Override
    public int compareTo(RecordId other){
        if (pageId != other.pageId) return Integer.compare(pageId, other.pageId);
        return Integer.compare(slotId, other.slotId);
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(pageId);
        buffer.putInt(slotId);
        return buffer.array();
    }

    public static RecordId deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new RecordId(buffer.getInt(), buffer.getInt());
    }

}
