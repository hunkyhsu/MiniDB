package com.hunkyhsu.minidb.tuple;

import com.hunkyhsu.minidb.storage.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Slotted Page 实现 - 用于存储变长 Tuple 的页面结构
 *
 * <p>页面布局 (4096 bytes):
 * <pre>
 * +----------------+------------------+----------------+------------------+
 * |     Header     |   Slot Array     |    Free Space  |   Tuple Data     |
 * |    (24 bytes)  |  (grows right →) |                |  (← grows left)  |
 * +----------------+------------------+----------------+------------------+
 * 0               24                                                       4096
 * </pre>
 *
 * <p>Header 结构 (24 bytes):
 * <ul>
 *   <li>PageId (4 bytes): 当前页面 ID</li>
 *   <li>PrevPageId (4 bytes): 前一页 ID，-1 表示无</li>
 *   <li>NextPageId (4 bytes): 后一页 ID，-1 表示无</li>
 *   <li>FreeSpacePointer (4 bytes): 空闲空间起始位置（从页尾向前）</li>
 *   <li>TupleCount (4 bytes): Tuple 数量（包括已删除的）</li>
 * </ul>
 *
 * <p>Slot 结构 (4 bytes each):
 * <ul>
 *   <li>Offset (2 bytes): Tuple 在页面中的偏移量，0 表示已删除</li>
 *   <li>Length (2 bytes): Tuple 长度</li>
 * </ul>
 *
 * @author hunkyhsu
 * @see Page
 */
public class TablePage {
    private static final Logger logger = LoggerFactory.getLogger(TablePage.class);
    // Header Offset
    private static final int OFFSET_PAGE_ID = 0;
    private static final int OFFSET_PREV_PAGE_ID = 4;
    private static final int OFFSET_NEXT_PAGE_ID = 8;
    private static final int OFFSET_FREE_SPACE = 12;
    private static final int OFFSET_TUPLE_COUNT = 16;
    private static final int HEADER_SIZE = 24; // 20 bytes data + 4 bytes LSN/Padding

    private static final int SLOT_SIZE = 4; // [offset(2B), size(2B)]

    private final Page page;

    public TablePage(Page page) {
        this.page = page;
    }

    public void init(int pageId, int prevPageId) {
        ByteBuffer buffer = page.getPageData();

        buffer.putInt(OFFSET_PAGE_ID, pageId);
        buffer.putInt(OFFSET_PREV_PAGE_ID, prevPageId);
        buffer.putInt(OFFSET_NEXT_PAGE_ID, -1);
        buffer.putInt(OFFSET_FREE_SPACE, Page.PAGE_SIZE);
        buffer.putInt(OFFSET_TUPLE_COUNT, 0);

        logger.debug("Initialized Table Page: pageId={}, prevPageId={}", pageId, prevPageId);
    }

    // core functions
    public int insertTuple(Tuple tuple) {
        if (tuple == null || tuple.getData() == null) {throw new IllegalArgumentException("tuple is null");}
        if (tuple.getSize() == 0) {throw new IllegalArgumentException("tuple is empty");}
        if (tuple.getSize() > Page.PAGE_SIZE - HEADER_SIZE - SLOT_SIZE) {throw new IllegalArgumentException("tuple size is too large");}
        if (getFreeSpace() < tuple.getSize() + SLOT_SIZE) {
            logger.debug("Page {} has not enough space to insert(tupleSize = {})", page.getPageId(), tuple.getSize());
            return -1;
        }
        ByteBuffer buffer = page.getPageData();
        byte[] data = tuple.getData();
        int slotId = getTupleCount();
        int newSpaceOffset = getFreeSpacePointer() - tuple.getSize();

        for (int i = 0; i < tuple.getSize(); i++) {
            buffer.put(newSpaceOffset + i, data[i]);
        }
        setFreeSpacePointer(newSpaceOffset);
        setSlot(slotId, newSpaceOffset, tuple.getSize());
        setTupleCount(getTupleCount() + 1);

        logger.debug("Insert tuple Info: pageId = {}, slotId = {}, offset = {}, size = {}",
                page.getPageId(), slotId, newSpaceOffset, tuple.getSize());
        return slotId;
    }

    public Tuple getTuple(int slotId) {
        int tupleCount = getTupleCount();
        if (slotId < 0 || slotId >= tupleCount) {return null;}
        if (isDeleted(slotId)) {return null;}

        int tupleOffset = getTupleOffset(slotId);
        int tupleSize = getTupleSize(slotId);
        byte[] data = new byte[tupleSize];
        ByteBuffer buffer = page.getPageData();

        for (int i = 0; i < tupleSize; i++) {
            data[i] = buffer.get(tupleOffset + i);
        }

        return new Tuple(new RecordId(page.getPageId(), slotId), data);
    }



    public Boolean markDeleted(int slotId) {
        int tupleCount = getTupleCount();
        if (slotId < 0 || slotId >= tupleCount) {return false;}
        if (isDeleted(slotId)) {return false;}
        int tupleOffset = getTupleOffset(slotId);
        setSlot(slotId, tupleOffset, 0);

        logger.debug("Marked Delete: pageId = {}, slotId = {}", page.getPageId(), slotId);
        return true;
    }

    public Boolean updateTuple(Tuple newTuple, int slotId) {
        int tupleCount = getTupleCount();
        if (slotId < 0 || slotId >= tupleCount) {return false;}
        if (isDeleted(slotId)) {return false;}

        if (newTuple == null || newTuple.getData() == null) {throw new IllegalArgumentException("tuple is null");}
        if (newTuple.getSize() == 0) {throw new IllegalArgumentException("tuple is empty");}
        if (newTuple.getSize() > getTupleSize(slotId)) {
            logger.warn("Update Failed: new tuple size {} > old tuple size {}", newTuple.getSize(), getTupleSize(slotId));
            return false;
        }

        int tupleOffset = getTupleOffset(slotId);
        ByteBuffer buffer = page.getPageData();
        byte[] data = newTuple.getData();
        for (int i = 0; i< newTuple.getSize(); i++) {
            buffer.put(tupleOffset + i, data[i]);
        }

        if (newTuple.getSize() != getTupleSize(slotId)) {
            setSlot(slotId, tupleOffset, newTuple.getSize());
        }

        logger.debug("Update tuple Info: pageId = {}, slotId = {}", page.getPageId(), slotId);
        return true;
    }

    public int deleteTuple(Tuple tuple) {
        if (tuple == null || tuple.getRecordId() == null) {return -1;}
        RecordId rid = tuple.getRecordId();
        if (rid.getPageId() != getPageId()) {return -1;}
        boolean deleted = markDeleted(rid.getSlotId());
        return deleted ? rid.getSlotId() : -1;
    }

    // Getter and Setter
    public int getPageId() {
        return page.getPageData().getInt(OFFSET_PAGE_ID);
    }

    public int getPrevPageId() {
        return page.getPageData().getInt(OFFSET_PREV_PAGE_ID);
    }

    public void setPrevPageId(int prevPageId) {
        page.getPageData().putInt(OFFSET_PREV_PAGE_ID, prevPageId);
    }

    public int getNextPageId() {
        return page.getPageData().getInt(OFFSET_NEXT_PAGE_ID);
    }

    public void setNextPageId(int nextPageId) {
        page.getPageData().putInt(OFFSET_NEXT_PAGE_ID, nextPageId);
    }

    private int getFreeSpacePointer() {
        return page.getPageData().getInt(OFFSET_FREE_SPACE);
    }

    private void setFreeSpacePointer(int ptr) {
        page.getPageData().putInt(OFFSET_FREE_SPACE, ptr);
    }

    public int getTupleCount() {
        return page.getPageData().getInt(OFFSET_TUPLE_COUNT);
    }

    private void setTupleCount(int count) {
        page.getPageData().putInt(OFFSET_TUPLE_COUNT, count);
    }

    private int getFreeSpace() {
        return getFreeSpacePointer() - (HEADER_SIZE + SLOT_SIZE * getTupleCount());
    }

    private int getSlotOffset(int slotId) {
        return HEADER_SIZE + SLOT_SIZE * slotId;
    }

    private int getTupleOffset(int slotId) {
        int offset = getSlotOffset(slotId);
        return page.getPageData().getShort(offset) & 0xFFFF;
    }

    private int getTupleSize(int slotId) {
        int offset = getSlotOffset(slotId);
        return page.getPageData().getShort(offset + 2) & 0xFFFF;
    }
    private void setSlot(int slotId, int offset, int slotSize) {
        int slotOffset = getSlotOffset(slotId);
        ByteBuffer buffer = page.getPageData();
        buffer.putShort(slotOffset, (short) offset);
        buffer.putShort(slotOffset + 2, (short) slotSize);
    }

    private boolean isDeleted(int slotId) {
        return getTupleSize(slotId) == 0;
    }
}
