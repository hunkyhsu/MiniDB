package com.hunkyhsu.minidb.access.heap;

import java.util.Arrays;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/3/18 21:40
 */
public class TupleDescriptor {

    private final ColumnType[] columnTypes;

    private final int[] fixedOffsets;
    private final int[] varlenIndices;

    private final int totalFixedSize;
    private final int varlenCount;

    public TupleDescriptor(ColumnType[] columnTypes) {
        this.columnTypes = columnTypes;
        int columnCount = columnTypes.length;
        this.fixedOffsets = new int[columnCount];
        this.varlenIndices = new int[columnCount];

        int currentFixedOffset = 0;
        int currentVarlenIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            ColumnType columnType = columnTypes[i];
            if (columnType.isFixed()) {
                fixedOffsets[i] = currentFixedOffset;
                varlenIndices[i] = -1;
                currentFixedOffset += columnType.getFixedLength();
            }
            else {
                fixedOffsets[i] = -1;
                varlenIndices[i] = currentVarlenIndex;
                currentVarlenIndex++;
            }
        }
        this.totalFixedSize = currentFixedOffset;
        this.varlenCount = currentVarlenIndex;
    }
    // Getter
    public int getColumnCount() {
        return columnTypes.length;
    }
    public ColumnType getColumnType(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnTypes.length) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + columnIndex);
        }
        return columnTypes[columnIndex];
    }
    public boolean isFixed(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnTypes.length) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + columnIndex);
        }
        return fixedOffsets[columnIndex] != -1;
    }
    public int getFixedOffset(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnTypes.length) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + columnIndex);
        }
        return fixedOffsets[columnIndex];
    }
    public int getVarlenIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnTypes.length) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + columnIndex);
        }
        return varlenIndices[columnIndex];
    }
    public int getTotalFixedSize() {
        return totalFixedSize;
    }
    public int getVarlenCount() {
        return varlenCount;
    }
    // Utility
    @Override
    // TupleDescriptor{columns=[INTEGER, VARCHAR(50), INTEGER], fixedSize=8B, varlenCount=1}
    public String toString() {
        StringBuilder sb = new StringBuilder("TupleDescriptor{");
        sb.append("columnTypes=[");
        for (int i = 0; i < columnTypes.length; i++) {
            sb.append(columnTypes[i].toString());
            if (i < columnTypes.length - 1) {sb.append(", ");}
        }
        sb.append("], ");
        sb.append("fixedOffsets=").append(totalFixedSize).append("B");
        sb.append(", varlenCount=").append(varlenCount);
        sb.append("}");
        return sb.toString();
    }

}
