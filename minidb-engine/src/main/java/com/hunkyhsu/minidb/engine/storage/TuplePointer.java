package com.hunkyhsu.minidb.engine.storage;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/3 12:53
 */
public final class TuplePointer {
    private TuplePointer() {}

    private static final int PAGE_ID_SHIFT = 32;

    public static long pack(int pageId, int offset) {
        return (((long) pageId) << PAGE_ID_SHIFT) | (offset & 0xFFFFFFFFL);
    }

    public static int getPageId(long pointer) {
        return (int) (pointer >>> PAGE_ID_SHIFT);
    }

    public static int getOffset(long pointer) {
        return (int) (pointer & 0xFFFFFFFFL);
    }
}
