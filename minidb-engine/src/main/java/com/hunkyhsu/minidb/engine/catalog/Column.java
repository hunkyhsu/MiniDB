package com.hunkyhsu.minidb.engine.catalog;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/6 21:54
 */
public record Column(
        String columnName,
        Type type,
        int offset
) { }
