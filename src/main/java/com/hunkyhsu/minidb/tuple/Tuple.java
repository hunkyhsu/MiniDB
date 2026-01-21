package com.hunkyhsu.minidb.tuple;

import lombok.Data;

@Data
public class Tuple {
    private RecordId recordId;
    private byte[] data;

    public Tuple(byte[] data) {
        this.data = data;
    }
    public Tuple(RecordId recordId, byte[] data) {
        this.recordId = recordId;
        this.data = data;
    }

    public int getSize() {
        return data.length;
    }
}
