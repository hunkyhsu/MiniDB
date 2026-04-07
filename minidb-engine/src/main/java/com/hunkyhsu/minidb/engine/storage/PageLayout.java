package com.hunkyhsu.minidb.engine.storage;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/3 12:53
 */
public final class PageLayout {
    private PageLayout() {}

    public static final int PAGE_SIZE = 8 * 1024; //8KB
    public static final int XMIN_OFFSET = 0;
    public static final int LEN_OFFSET = Long.BYTES; // 8
    public static final int HEADER_SIZE = Long.BYTES + Integer.BYTES; // 12

}
