package com.hunkyhsu.minidb.access.heap;


import java.util.Arrays;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/3/16 00:33
 */
public class TupleHeader {

    public static final int FIXED_HEADER_SIZE = 24;
    public static final int MAX_COLUMNS = 1024;

    // Infomask
    private static final short FLAG_HAS_NULL = 0x0001;
    private static final short FLAG_HAS_VARLEN =  0x0002;
    private static final short FLAG_XMIN_COMMITTED = 0x0004;
    private static final short FLAG_XMIN_INVALID = 0x0008;
    private static final short FLAG_XMAX_COMMITTED = 0x0010;
    private static final short FLAG_XMAX_INVALID = 0x0020;
    private static final short FLAG_UPDATED = 0x0040;

    private int xmin;
    private int xmax;
    private int cid;
    private int ctidPageId;
    private short ctidSlotId;
    private short infomask;
    private short columnsCount;
    private short headerOffset;
    private byte[] nullBitMap;

    public TupleHeader(int columnsCount) {
        if (columnsCount < 0 || columnsCount > MAX_COLUMNS) {
            throw new IllegalArgumentException(String.format("column count should be in [0, %d]", MAX_COLUMNS));
        }
        int bitMapBytes = (columnsCount + 7) / Byte.SIZE;
        int totalHeaderOffset = FIXED_HEADER_SIZE + bitMapBytes;
        this.nullBitMap = new byte[bitMapBytes];

        reset(0, 0, 0, -1,
                (short)-1, (short)0, (short) columnsCount,
                (short) totalHeaderOffset);
    }

    public void reset(int xmin, int xmax, int cid, int ctidPageId
    , short ctidSlotId, short infomask, short columnsCount, short headerOffset) {
        if (columnsCount < 0 || columnsCount > MAX_COLUMNS) {
            throw new IllegalArgumentException(String.format("column count should be in [0, %d]", MAX_COLUMNS));
        }
        this.xmin = xmin;
        this.xmax = xmax;
        this.cid = cid;
        this.ctidPageId = ctidPageId;
        this.ctidSlotId = ctidSlotId;
        this.infomask = infomask;
        this.columnsCount = columnsCount;
        this.headerOffset = headerOffset;
        Arrays.fill(nullBitMap, (byte) 0);
    }

    // Flags Setting
    private void setFlag(short flag) {
        this.infomask |= flag;
    }

    private void clearFlag(short flag) {
        this.infomask &= ~flag;
    }

    // Null Status Management
    public boolean isNull(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnsCount) {
            throw new IllegalArgumentException(String.format("column index should be in [0, %d)", columnsCount));
        }
        if (nullBitMap == null) {
            throw new IllegalStateException("null bitmap is not initialized");
        }
        if (nullBitMap.length == 0) {
            return false;
        }

        int byteIndex = columnIndex / Byte.SIZE;
        int bitIndex = columnIndex % Byte.SIZE;
        return (nullBitMap[byteIndex] & (1 << bitIndex)) != 0;
    }

    public void setNull(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnsCount) {
            throw new IllegalArgumentException(String.format("column index should be in [0, %d)", columnsCount));
        }
        if (nullBitMap == null) {
            throw new IllegalStateException("null bitmap is not initialized");
        }

        int byteIndex = columnIndex / Byte.SIZE;
        int bitIndex = columnIndex % Byte.SIZE;
        nullBitMap[byteIndex] |= (byte) (1 << bitIndex);
        setFlag(FLAG_HAS_NULL);
    }

    public boolean hasNull() {
        return (infomask & FLAG_HAS_NULL) != 0;
    }

    // Getter & Setter
    public int getXmin() { return xmin; }
    public void setXmin(int xmin) { this.xmin = xmin; }

    public int getXmax() { return xmax; }
    public void setXmax(int xmax) { this.xmax = xmax; }

    public int getCid() { return cid; }
    public void setCid(int cid) { this.cid = cid; }

    public int getCtidPageId() { return ctidPageId; }
    public void setCtidPageId(int ctidPageId) { this.ctidPageId = ctidPageId; }

    public short getCtidSlotId() { return ctidSlotId; }
    public void setCtidSlotId(short ctidSlotId) { this.ctidSlotId = ctidSlotId; }

    public short getColumnsCount() { return columnsCount; }
    public short getHeaderOffset() { return headerOffset; }

    // MVCC Setting
    void setHasVarlen(){
        setFlag(FLAG_HAS_VARLEN);
    }

    public void setUpdated(){
        setFlag(FLAG_UPDATED);
    }

    public void setXminCommitted() {
        // 置位 COMMITTED，强制清除 INVALID，保障状态互斥
        this.infomask = (short) ((this.infomask & ~FLAG_XMIN_INVALID) | FLAG_XMIN_COMMITTED);
    }

    public void setXminInvalid() {
        // 置位 INVALID，强制清除 COMMITTED
        this.infomask = (short) ((this.infomask & ~FLAG_XMIN_COMMITTED) | FLAG_XMIN_INVALID);
    }

    public void setXmaxCommitted() {
        this.infomask = (short) ((this.infomask & ~FLAG_XMAX_INVALID) | FLAG_XMAX_COMMITTED);
    }

    public void setXmaxInvalid() {
        this.infomask = (short) ((this.infomask & ~FLAG_XMAX_COMMITTED) | FLAG_XMAX_INVALID);
    }

    public boolean isHasVarlen() { return (infomask & FLAG_HAS_VARLEN) != 0; }
    public boolean isXminCommitted() { return (infomask & FLAG_XMIN_COMMITTED) != 0; }
    public boolean isXminInvalid() { return (infomask & FLAG_XMIN_INVALID) != 0; }
    public boolean isXmaxCommitted() { return (infomask & FLAG_XMAX_COMMITTED) != 0; }
    public boolean isXmaxInvalid() { return (infomask & FLAG_XMAX_INVALID) != 0; }
    public boolean isUpdated() { return (infomask & FLAG_UPDATED) != 0; }
    // Package-private API: Trusted Scope
    short getRawInfomask() {
        return infomask;
    }

    byte[] getRawNullBitmap() {
        return nullBitMap;
    }

}
