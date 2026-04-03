package com.hunkyhsu.minidb.access.heap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/3/16 00:33
 */
public class TupleCodec {
    private static final int MAX_TUPLE_SIZE = 4000;

    public static int encode(TupleDescriptor descriptor, Object[] values
                            , ByteBuffer targetBuffer, int targetOffset) {
        if (values == null) {
            throw new IllegalArgumentException("values is null");
        }
        if (values.length != descriptor.getColumnCount()) {
            throw new IllegalArgumentException("values.length != descriptor.getColumnCount()");
        }
        int notNullVarlenCount = 0;
        int varlenTotalSize = 0;
        byte[][] varlenCache = new byte[values.length][];

        TupleHeader header = new TupleHeader(descriptor.getColumnCount());

        for (int i = 0; i < values.length; i++) {
            if (!descriptor.isFixed(i) && values[i] != null) {
                header.setHasVarlen();
                ColumnType.TypeId typeId = descriptor.getColumnType(i).getTypeId();
                if (typeId == ColumnType.TypeId.VARCHAR) {
                    varlenCache[i] = ((String) values[i]).getBytes(StandardCharsets.UTF_8);
                }else {
                    throw new IllegalArgumentException("Unsupported varlen type: " + typeId);
                }
                notNullVarlenCount++;
                varlenTotalSize += varlenCache[i].length;
            }
            if (values[i] == null) {
                header.setNull(i);
            }
        }
        int headerSize = TupleHeader.FIXED_HEADER_SIZE + header.getRawNullBitmap().length;
        int fixedSize = descriptor.getTotalFixedSize();
        int offsetArraySize = notNullVarlenCount * 2;
        int totalTupleSize = headerSize + fixedSize + offsetArraySize + varlenTotalSize;
        if (totalTupleSize > MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException("totalTupleSize > MAX_TUPLE_SIZE: " + MAX_TUPLE_SIZE);
        }
        if (targetBuffer.capacity() - targetOffset < totalTupleSize) {
            throw new IllegalArgumentException("targetOffset does not have enough space for targetBuffer.capacity(): " + targetBuffer.capacity());
        }

        // write to target buffer: Sequential access > Small loop count optimize
        // 1. write header
        int originalPosition = targetBuffer.position();
        targetBuffer.position(targetOffset);
        targetBuffer.putInt(header.getXmin());
        targetBuffer.putInt(header.getXmax());
        targetBuffer.putInt(header.getCid());
        targetBuffer.putInt(header.getCtidPageId());
        targetBuffer.putShort(header.getCtidSlotId());
        targetBuffer.putShort(header.getRawInfomask());
        targetBuffer.putShort(header.getColumnsCount());
        targetBuffer.putShort(header.getHeaderOffset());
        targetBuffer.put(header.getRawNullBitmap());

        int fixedOffset = targetOffset + header.getHeaderOffset();
        int offsetArrayOffset = fixedOffset + descriptor.getTotalFixedSize();
        int varlenOffset = offsetArrayOffset + offsetArraySize;
        int currentRelativeOffset = 0;
        // 2. write fixed data
        for (int i = 0; i < values.length; i++) {
            if (descriptor.isFixed(i)) {
                if (values[i] != null) {
                    // Using switch when more types support
                    ColumnType.TypeId typeId = descriptor.getColumnType(i).getTypeId();
                    switch (typeId) {
                        case INTEGER ->  targetBuffer.putInt((Integer) values[i]);
                        default -> throw new IllegalArgumentException("Unsupported fixed type: " + typeId);
                    }
                }
                else {
                    int len = descriptor.getColumnType(i).getFixedLength();
                    for (int j = 0; j < len; j++) {targetBuffer.putInt((byte) 0);}
                }
            }
        }
        // 3. write offset array
        for (int i = 0; i < values.length; i++) {
            if (!descriptor.isFixed(i)) {
                if (values[i] != null) {
                    currentRelativeOffset += varlenCache[i].length;
                }
                targetBuffer.putShort((short) currentRelativeOffset);
            }
        }
        // 4. write varlen data
        for (int i = 0; i < values.length; i++) {
            if (!descriptor.isFixed(i) && values[i] != null) {
                targetBuffer.put(varlenCache[i]);
            }
        }
        targetBuffer.position(originalPosition);
        return totalTupleSize;
    }

    public static int getInt(Tuple tuple, int logicalIndex, TupleDescriptor descriptor) {
        TupleHeader header = tuple.getTupleHeader();
        if (header.isNull(logicalIndex)) {
            throw new IllegalArgumentException("Data is null");
        }
        if (!descriptor.isFixed(logicalIndex)) {
            throw new IllegalArgumentException("Data is not fixed");
        }
        int fixedOffset = tuple.getTupleOffset() + header.getHeaderOffset();
        int dataOffset = fixedOffset + descriptor.getFixedOffset(logicalIndex);
        return tuple.getPageData().getInt(dataOffset);
    }

    public static ByteBuffer getVarcharView(Tuple tuple, int logicalIndex, TupleDescriptor descriptor) {
        TupleHeader header = tuple.getTupleHeader();
        if (header.isNull(logicalIndex)) {
            throw new IllegalArgumentException("Data is null");
        }
        if (descriptor.isFixed(logicalIndex)) {
            throw new IllegalArgumentException("Data is not varchar");
        }
        // seek for offset array first
        int arrayOffset = tuple.getTupleOffset() + header.getHeaderOffset() + descriptor.getTotalFixedSize();
        int varlenIndex = descriptor.getVarlenIndex(logicalIndex);
        short varlenStart = varlenIndex == 0 ? 0 : tuple.getPageData().getShort(arrayOffset + (varlenIndex - 1) * 2);
        short varlenEnd = tuple.getPageData().getShort(arrayOffset + varlenIndex * 2);
        // seek for varlen data
        int varlenDataOffset = arrayOffset + descriptor.getVarlenCount() * 2;
        ByteBuffer buffer = tuple.getPageData().asReadOnlyBuffer();
        buffer.position(varlenDataOffset + varlenStart);
        buffer.limit(varlenDataOffset + varlenEnd);
        return buffer.slice();
    }
}
