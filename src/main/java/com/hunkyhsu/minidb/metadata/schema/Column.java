package com.hunkyhsu.minidb.metadata.schema;

import com.hunkyhsu.minidb.metadata.Type;
import lombok.Getter;


@Getter
public class Column {
    private final String name;
    private final Type type;
    private final boolean nullable;

    public Column(String name, Type type, boolean nullable) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Column name is empty");
        }
        if (type == null) {
            throw new IllegalArgumentException("Column type is null");
        }
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

}
