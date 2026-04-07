package com.hunkyhsu.minidb.engine.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/3 18:47
 */
class TuplePointerTest {

    @Test
    @DisplayName("Normal Pointer")
    void testNormalPointer() {
        int expectedPageId = 128;
        int expectedOffset = 4096;
        long pointer = TuplePointer.pack(expectedPageId, expectedOffset);
        assertEquals(expectedPageId, TuplePointer.getPageId(pointer), "PageId Unpack failed");
        assertEquals(expectedOffset, TuplePointer.getOffset(pointer), "Offset Unpack failed");
    }

    @Test
    @DisplayName("Zero Pointer")
    void testZeroPointer() {
        long pointer = TuplePointer.pack(0, 0);
        assertEquals(0, TuplePointer.getPageId(pointer), "PageId Unpack failed");
        assertEquals(0, TuplePointer.getOffset(pointer), "Offset Unpack failed");
    }

    @Test
    @DisplayName("Extreme Pointer")
    void testExtremesPointer() {
        int expectedPageId = 999999;
        int expectedOffset = PageLayout.PAGE_SIZE - 1;
        long pointer = TuplePointer.pack(expectedPageId, expectedOffset);
        assertEquals(expectedPageId, TuplePointer.getPageId(pointer), "PageId Unpack failed");
        assertEquals(expectedOffset, TuplePointer.getOffset(pointer), "Offset Unpack failed");
    }

    @Test
    @DisplayName("Max Int Pointer")
    void testMaxIntPointer() {
        int expectedPageId = Integer.MAX_VALUE;
        int expectedOffset = Integer.MAX_VALUE;
        long pointer = TuplePointer.pack(expectedPageId, expectedOffset);
        assertEquals(expectedPageId, TuplePointer.getPageId(pointer), "PageId Unpack failed");
        assertEquals(expectedOffset, TuplePointer.getOffset(pointer), "Offset Unpack failed");
    }

    @Test
    @DisplayName("Negative Pointer")
    void testNegativePointer() {
        int expectedPageId = -1;
        int expectedOffset = 256;
        long pointer = TuplePointer.pack(expectedPageId, expectedOffset);
        assertEquals(expectedPageId, TuplePointer.getPageId(pointer), "PageId Unpack failed");
        assertEquals(expectedOffset, TuplePointer.getOffset(pointer), "Offset Unpack failed");
    }


}
