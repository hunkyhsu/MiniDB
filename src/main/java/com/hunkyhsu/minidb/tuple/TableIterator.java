package com.hunkyhsu.minidb.tuple;

import com.hunkyhsu.minidb.storage.BufferPoolManager;
import com.hunkyhsu.minidb.storage.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class TableIterator implements Iterator<Tuple> {
    private static final Logger logger = LoggerFactory.getLogger(TableIterator.class);
    private final BufferPoolManager bufferPoolManager;

    private int currentPageId;
    private int currentSlotId;
    private Tuple nextTuple;

    public TableIterator(BufferPoolManager bufferPoolManager, int firstPageId) {
        this.bufferPoolManager = bufferPoolManager;
        this.currentPageId = firstPageId;
        this.currentSlotId = 0;
        this.nextTuple = fetchNextTuple();
    }

    private Tuple fetchNextTuple() {
        while (currentPageId != -1) {
            try {
                Page page = bufferPoolManager.fetchPage(currentPageId);
                if (page == null) { currentPageId = -1; return null; }

                TablePage tablePage = new TablePage(page);
                int tupleCount = tablePage.getTupleCount();

                // range all the slot in current page
                while (currentSlotId < tupleCount) {
                    Tuple tuple = tablePage.getTuple(currentSlotId);
                    currentSlotId++;
                    if (tuple != null) {
                        bufferPoolManager.unpinPage(currentPageId, false);
                        return tuple;
                    }
                }
                // find in next page from slot0
                int nextPageId = tablePage.getNextPageId();
                bufferPoolManager.unpinPage(currentPageId, false);
                currentPageId = nextPageId;
                currentSlotId = 0;

            } catch (IOException e) {
                logger.error("Failed to fetch next tuple", e);
                return null;
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return nextTuple != null;
    }

    @Override
    public Tuple next() {
        if (nextTuple == null) {
            throw new NoSuchElementException("TableIterator exhausted");
        }
        Tuple tuple = nextTuple;
        nextTuple = fetchNextTuple();
        return tuple;
    }
}
