package com.hunkyhsu.minidb.access.record;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class RecordId implements Serializable, Comparable<RecordId> {
    private final int pageId;
    private final short slotId;

    @Override
    public int compareTo(RecordId other){
        if (pageId != other.pageId) return Integer.compare(pageId, other.pageId);
        return Integer.compare(slotId, other.slotId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordId recordId = (RecordId) o;
        return pageId == recordId.pageId && slotId == recordId.slotId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, slotId);
    }

    @Override
    public String toString() {
        return String.format("[pageId=%d, slotId=%d]", pageId, slotId);
    }

}
