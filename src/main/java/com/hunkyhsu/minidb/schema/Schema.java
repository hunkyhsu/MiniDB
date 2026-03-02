package com.hunkyhsu.minidb.schema;

import com.hunkyhsu.minidb.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * [null bitmap][fixed-size区][varlen offset表][varlen data]
 */
public class Schema {
    private final List<Column> columns;
    // map <column name, column index>
    private final Map<String, Integer> columnIndex;

    public Schema(List<Column> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Schema columns are empty");
        }
        this.columns = new ArrayList<>(columns);
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (index.containsKey(column.getName())) {
                throw new IllegalArgumentException("Duplicate column name: " + column.getName());
            }
            index.put(column.getName(), i);
        }
        this.columnIndex = index;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public int getColumnIndex(String name) {
        Integer index = columnIndex.get(name);
        if (index == null) {
            throw new IllegalArgumentException("Unknown column: " + name);
        }
        return index;
    }

    public boolean isCompatible(Schema other) {
        if (other == null || other.getColumnCount() != getColumnCount()) {
            return false;
        }
        for (int i = 0; i < columns.size(); i++) {
            Type thisType = columns.get(i).getType();
            Type otherType = other.columns.get(i).getType();
            if (!thisType.isCompatible(otherType)) {
                return false;
            }
        }
        return true;
    }
}
