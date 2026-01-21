package com.hunkyhsu.minidb.tuple;

import com.hunkyhsu.minidb.storage.BufferPoolManager;
import com.hunkyhsu.minidb.storage.Page;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * TableHeap - 堆表管理器
 *
 * <p>管理一张表的所有数据页，通过双向链表组织页面。
 * 提供 Tuple 级别的 CRUD 操作和全表扫描能力。
 *
 * <h2>页面组织结构</h2>
 * <pre>
 * FirstPage ←→ Page1 ←→ ... ←→ LastPage
 *    ↑                           ↑
 * firstPageId                 lastPageId
 * </pre>
 *
 * <h2>线程安全</h2>
 * <p>本类不是线程安全的。在并发环境中，调用方需通过 LockManager 加锁。
 *
 * @author hunkyhsu
 * @see TablePage
 * @see BufferPoolManager
 */
public class TableHeap {
    private static final Logger logger = LoggerFactory.getLogger(TableHeap.class);
    private final BufferPoolManager bufferPoolManager;
    @Getter
    private int firstPageId;
    @Getter
    private int lastPageId;

    public TableHeap(BufferPoolManager bufferPoolManager) {
        this.bufferPoolManager = bufferPoolManager;
        try {
            Page page = bufferPoolManager.newPage();
            this.firstPageId = page.getPageId();
            this.lastPageId = page.getPageId();
            TablePage tablePage = new TablePage(page);
            tablePage.init(page.getPageId(), -1);
            bufferPoolManager.unpinPage(page.getPageId(), true);

            logger.info("Create new Table Heap: firstPageId = {}, lastPageId = {}", firstPageId, lastPageId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Table Heap", e);
        }
    }

    public TableHeap(BufferPoolManager bufferPoolManager, int firstPageId) {
        this.firstPageId = firstPageId;
        this.bufferPoolManager = bufferPoolManager;
        this.lastPageId = firstPageId;
        int currentPageId = firstPageId;
        try {
            while (currentPageId != -1) {
                this.lastPageId = currentPageId;
                Page page = bufferPoolManager.fetchPage(currentPageId);
                if (page == null) {break;}
                TablePage tablePage = new TablePage(page);
                int nextPageId = tablePage.getNextPageId();
                bufferPoolManager.unpinPage(currentPageId, false);
                currentPageId = nextPageId;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Table Heap", e);
        }
    }



    public RecordId insertTuple(Tuple tuple) {
        if (tuple == null || tuple.getData() == null) {
            throw new IllegalArgumentException("tuple is null");
        }
        try {
            Page lastPage = bufferPoolManager.fetchPage(lastPageId);
            if (lastPage == null) {throw new IllegalArgumentException("lastPage is null");}
            TablePage tablePage = new TablePage(lastPage);
            int slotId = tablePage.insertTuple(tuple);
            // case 1: insert success
            if (slotId != -1) {
                RecordId rid = new RecordId(lastPageId, slotId);
                tuple.setRecordId(rid);
                bufferPoolManager.unpinPage(lastPageId, true);
                return rid;
            }
            // case 2: need to allocate new page
            Page newPage = bufferPoolManager.newPage();
            if (newPage == null) {throw new IllegalArgumentException("newPage is null");}

            int newPageId = newPage.getPageId();
            TablePage newTablePage = new TablePage(newPage);
            newTablePage.init(newPageId, lastPageId);
            tablePage.setNextPageId(newPageId);

            int newSlotId = newTablePage.insertTuple(tuple);
            RecordId rid = new RecordId(newPageId, newSlotId);
            tuple.setRecordId(rid);
            this.lastPageId = newPageId;

            bufferPoolManager.unpinPage(tablePage.getPageId(), true);
            bufferPoolManager.unpinPage(newPageId, true);

            return rid;

        } catch (IOException e) {
            throw new RuntimeException("Failed to insert Table Heap", e);
        }
    }

    public Tuple getTuple(RecordId recordId) {
        if (recordId == null) {return null;}
        try {
            Page page = bufferPoolManager.fetchPage(recordId.getPageId());
            TablePage tablePage = new TablePage(page);
            Tuple tuple = tablePage.getTuple(recordId.getSlotId());
            bufferPoolManager.unpinPage(recordId.getPageId(), false);
            return tuple;
        } catch (IOException e) {
            logger.error("Failed to get tuple from Table Heap", e);
            return null;
        }
    }

    public Boolean markDeleted(RecordId recordId) {
        if (recordId == null) {return false;}
        try {
            Page page = bufferPoolManager.fetchPage(recordId.getPageId());
            TablePage tablePage = new TablePage(page);
            Boolean deleted = tablePage.markDeleted(recordId.getSlotId());
            bufferPoolManager.unpinPage(recordId.getPageId(), true);
            return deleted;
        } catch (IOException e) {
            logger.error("Failed to mark deleted Table Heap", e);
            return false;
        }
    }

    public Boolean updateTuple(RecordId recordId, Tuple newTuple) {
        if (recordId == null || newTuple == null) {return false;}
        try {
            Page page = bufferPoolManager.fetchPage(recordId.getPageId());
            TablePage tablePage = new TablePage(page);
            Boolean result = tablePage.updateTuple(newTuple, recordId.getSlotId());
            bufferPoolManager.unpinPage(recordId.getPageId(), true);
            return result;
        } catch (IOException e) {
            logger.error("Failed to update tuple from Table Heap", e);
            return false;
        }
    }

    public TableIterator iterator() {
        return new TableIterator(this.bufferPoolManager, this.firstPageId);
    }

}
