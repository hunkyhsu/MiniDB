package com.hunkyhsu.minidb.engine.catalog;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/6 21:54
 */
public class CatalogManager {
    private final ConcurrentMap<String, TableMetadata> tables;
    public CatalogManager() {
        tables = new ConcurrentHashMap<>();
    }
    public void createTable(String tableName, List<Column> columns) {
        if (tables.putIfAbsent(tableName, new TableMetadata(tableName, columns)) != null) {
            throw new IllegalArgumentException("Table already exists: " + tableName);
        }
    }
    public TableMetadata getTable(String tableName) {
        if (!tables.containsKey(tableName)) { throw new NoSuchElementException(tableName); }
        return tables.get(tableName);
    }
}
