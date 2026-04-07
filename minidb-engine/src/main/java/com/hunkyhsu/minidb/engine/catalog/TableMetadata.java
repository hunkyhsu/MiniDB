package com.hunkyhsu.minidb.engine.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/6 21:54
 */
public class TableMetadata {
    private final String tableName;
    private final List<Column> columns;
    private final int tupleSize;

    public TableMetadata(String tableName, List<Column> columns) {
        this.tableName = tableName;
        List<Column> fixedColumns = new ArrayList<>();
        int currentOffset = 0;
        for (Column column : columns) {
            fixedColumns.add(new Column(column.columnName(), column.type(), currentOffset));
            currentOffset += column.type().getFixedSize();
        }
        this.columns = Collections.unmodifiableList(fixedColumns);
        this.tupleSize = currentOffset;
    }

    public int getColumnOffset(String columnName) {
        for (Column column : columns) {
            if (columnName.equals(column.columnName())) {
                return column.offset();
            }
        }
        throw new NoSuchElementException("Column not found: " + columnName);
    }

    public String getTableName() {
        return tableName;
    }
    public List<Column> getColumns() {
        return columns;
    }
    public int getTupleSize() {
        return tupleSize;
    }

}
