package com.hunkyhsu.minidb.engine.catalog;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/6 21:54
 */
public enum Type {
    INT(4, true);
    private final int fixedSize;
    private final boolean isFixed;
    Type(int fixedSize, boolean isFixed) {
        this.fixedSize = fixedSize;
        this.isFixed = isFixed;
    }
    public int getFixedSize() {
        return fixedSize;
    }
    public boolean isFixed() {
        return isFixed;
    }
}
