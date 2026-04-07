package com.hunkyhsu.minidb.engine.storage;

import java.security.InvalidParameterException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/4 15:51
 */
public class AppendOnlyTableStore {
    private final MMapFileChannel channel;
    private final AtomicInteger currentOffset;

    public AppendOnlyTableStore(MMapFileChannel channel) {
        this.channel = channel;
        this.currentOffset = new AtomicInteger(0);
    }

    public long insertTuple(long xmin, byte[] payload) {
        int totalTupleSize = PageLayout.HEADER_SIZE + payload.length;
        if (totalTupleSize > PageLayout.PAGE_SIZE) {
            throw new IllegalArgumentException("Not Support Oversized Tuple: " + totalTupleSize + " > " + PageLayout.PAGE_SIZE);
        }
        int absOffset;
        int current;
        int next;
        // No-lock CAS Spin Loop
        do {
            current = currentOffset.get();
            int takenPageSize = current % PageLayout.PAGE_SIZE;
            if (totalTupleSize > PageLayout.PAGE_SIZE - takenPageSize) {
                absOffset = ((current / PageLayout.PAGE_SIZE) + 1) * PageLayout.PAGE_SIZE;
            } else {
                absOffset = current;
            }
            next = absOffset + totalTupleSize;
        } while (!currentOffset.compareAndSet(current, next));
        int pageId = absOffset / PageLayout.PAGE_SIZE;
        int offset = absOffset % PageLayout.PAGE_SIZE;
        long pointer = TuplePointer.pack(pageId, offset);
        channel.writeTuple(pointer, xmin, payload);
        return pointer;
    }

    public int getValidEndOffset() {
        return currentOffset.get();
    }
}
