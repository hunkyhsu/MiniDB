package com.hunkyhsu.minidb.access.heap;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/3/18 21:35
 */
public class ColumnType {
    private static final int VARCHAR_MAX_LENGTH = 1024;

    public enum TypeId {
        INTEGER,
        VARCHAR
    }

    private final TypeId typeId;
    private final int maxLength; // For varchar
    private final int fixedLength;
    private final boolean isFixed;

    ColumnType(TypeId typeId, int maxLength, int fixedLength, boolean isFixed) {
        this.fixedLength = fixedLength;
        this.isFixed = isFixed;
        this.typeId = typeId;
        this.maxLength = maxLength;
    }

    // static factory:
    public static ColumnType INTEGER() {
        return new ColumnType(TypeId.INTEGER, 4, 4, true);
    }
    public static ColumnType VARCHAR(int maxLength) {
        if (maxLength <= 0 || maxLength > VARCHAR_MAX_LENGTH) {
            throw new IllegalArgumentException("maxLength must be between 1 and 4000");
        }
        return new ColumnType(TypeId.VARCHAR, maxLength, -1, false);
    }

    // Getter
    public int getFixedLength() { return fixedLength; }
    public boolean isFixed() { return isFixed; }
    public TypeId getTypeId() { return typeId; }
    public int getMaxLength() { return maxLength; }

    // Utility
    @Override
    public String toString() {
        if (typeId == TypeId.VARCHAR) {
            return "VARCHAR(" + maxLength + ")";
        }
        return typeId.name();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnType that = (ColumnType) o;
        return maxLength == that.maxLength && typeId == that.typeId;
    }
    @Override
    public int hashCode() {
        return java.util.Objects.hash(typeId, maxLength);
    }

}
