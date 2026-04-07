package com.hunkyhsu.minidb.engine.execution;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/5 21:12
 */
public interface DbIterator extends AutoCloseable {
    public static final long EOF = -1L;
    void open();
    long next();
    void close();
}
