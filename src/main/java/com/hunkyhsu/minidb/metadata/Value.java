package com.hunkyhsu.minidb.metadata;

public class Value {
    private final TypeId typeId;
    private final Object value;

    private Value(TypeId typeId, Object value) {
        this.typeId = typeId;
        this.value = value;
    }

    public static Value ofInt(int value) {
        return new Value(TypeId.INT, value);
    }

    public static Value ofVarchar(String value) {
        return new Value(TypeId.VARCHAR, value);
    }

    public static Value nullValue(TypeId typeId) {
        return new Value(typeId, null);
    }

    public TypeId getTypeId() {
        return typeId;
    }

    public boolean isNull() {
        return value == null;
    }

    public int asInt() {
        if (typeId != TypeId.INT) {
            throw new IllegalStateException("Value is not INT");
        }
        if (value == null) {
            throw new IllegalStateException("INT value is null");
        }
        return (Integer) value;
    }

    public String asVarchar() {
        if (typeId != TypeId.VARCHAR) {
            throw new IllegalStateException("Value is not VARCHAR");
        }
        if (value == null) {
            throw new IllegalStateException("VARCHAR value is null");
        }
        return (String) value;
    }

    @Override
    public String toString() {
        if (value == null) {
            return "NULL";
        }
        return value.toString();
    }
}
