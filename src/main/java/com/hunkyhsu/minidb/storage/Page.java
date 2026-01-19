package com.hunkyhsu.minidb.storage;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

@Getter
public class Page {
    public static final int PAGE_SIZE = 4096; // 4kb
    @Setter
    private int pageId = -1;

    private final ByteBuffer pageData;
    @Setter
    private boolean isDirty = false;

    private int pinCount = 0;

    public Page() {
        this.pageData = ByteBuffer.allocate(PAGE_SIZE);
        resetMemory();
    }

    public void resetMemory() {
        this.pageId = -1;
        this.isDirty = false;
        this.pinCount = 0;
        this.pageData.clear();
    }

    /**
     * increase the pinCount
     */
    public void pin() {
        this.pinCount++;
    }

    /**
     * decrease the pinCount
     */
    public void unpin() {
        if (this.pinCount > 0) {
            this.pinCount--;
        }
    }
}
