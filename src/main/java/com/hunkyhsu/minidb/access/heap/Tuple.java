package com.hunkyhsu.minidb.access.heap;

import com.hunkyhsu.minidb.access.record.RecordId;

import java.nio.ByteBuffer;

public class Tuple {
    private int pageId;
    private short slotId;
    private final TupleHeader tupleHeader;
    private int size;
    private int tupleOffset;
    private ByteBuffer pageData;

    public Tuple() {
        this.pageId = -1;
        this.slotId = -1;
        this.tupleHeader = new TupleHeader(TupleHeader.MAX_COLUMNS);
    }

    public void reset(int pageId, short slotId, int size, int tupleOffset, ByteBuffer pageData) {
        this.pageId = pageId;
        this.slotId = slotId;
        this.size = size;
        this.tupleOffset = tupleOffset;
        this.pageData = pageData;
        parseHeader();
    }

    private void parseHeader() {
        int originPos = pageData.position();
        try {
            pageData.position(tupleOffset);
            int xmin = pageData.getInt();
            int xmax = pageData.getInt();
            int cid = pageData.getInt();
            int ctidPageId = pageData.getInt();
            short ctidSlotId = pageData.getShort();
            short infomask = pageData.getShort();
            short columnsCount = pageData.getShort();
            short headerOffset = pageData.getShort();

            if (columnsCount < 0 || columnsCount > TupleHeader.MAX_COLUMNS) {
                throw new IllegalArgumentException("Invalid columnsCount: " + columnsCount);
            }
            if (headerOffset < TupleHeader.FIXED_HEADER_SIZE) {
                throw new IllegalArgumentException("Invalid headerOffset: " + headerOffset);
            }

            tupleHeader.reset(xmin, xmax, cid, ctidPageId, ctidSlotId, infomask, columnsCount, headerOffset);
            if (tupleHeader.hasNull()) {
                int actualBitmapBytes = (columnsCount + 7) / 8;
                byte[] rawBitmap = tupleHeader.getRawNullBitmap();
                pageData.get(rawBitmap, 0, actualBitmapBytes);
            }

        } finally {
            pageData.position(originPos);
        }
    }

    public boolean isNull(int columnIndex) {
        return tupleHeader.isNull(columnIndex);
    }

    // MVCC
    public int getXmin() { return tupleHeader.getXmin(); }
    public int getXmax() { return tupleHeader.getXmax();}
    public int getCid() { return tupleHeader.getCid(); }

    public boolean isXminCommitted() { return tupleHeader.isXminCommitted(); }
    public boolean isXminInvalid() { return tupleHeader.isXminInvalid(); }
    public boolean isXmaxCommitted() { return tupleHeader.isXmaxCommitted(); }
    public boolean isXmaxInvalid() { return tupleHeader.isXmaxInvalid(); }
    public boolean isUpdated() { return tupleHeader.isUpdated(); }

    // Getter
    public ByteBuffer getPageData() {
        return pageData;
    }
    public int getTupleOffset() {
        return tupleOffset;
    }
    public TupleHeader getTupleHeader() {
        return tupleHeader;
    }
    public RecordId getRecordId() { return new RecordId(pageId, slotId); }
    public int getSize() { return size; }

    // External interface
    public int getInt(int logicalIndex, TupleDescriptor descriptor) {
        validateColumnAccess(logicalIndex, descriptor, true);
        return TupleCodec.getInt(this, logicalIndex, descriptor);
    }
    public ByteBuffer getVarcharView(int logicalIndex, TupleDescriptor descriptor) {
        validateColumnAccess(logicalIndex, descriptor, false);
        return TupleCodec.getVarcharView(this, logicalIndex, descriptor);
    }

    private void validateColumnAccess(int columnIndex, TupleDescriptor descriptor, boolean isFixed) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor cannot be null");
        }
        if (columnIndex < 0 || columnIndex >= descriptor.getColumnCount()) {
            throw new IllegalArgumentException("columnIndex out of bounds: " + columnIndex);
        }
        if (tupleHeader.getColumnsCount() != descriptor.getColumnCount()) {
            throw new IllegalArgumentException("descriptor column count mismatch");
        }
        if (descriptor.isFixed(columnIndex) != isFixed) {
            throw new IllegalArgumentException("Column type mismatch at index " + columnIndex);
        }
    }
}
