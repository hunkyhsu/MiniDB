package com.hunkyhsu.minidb.engine.execution;

import com.hunkyhsu.minidb.engine.storage.AppendOnlyTableStore;
import com.hunkyhsu.minidb.engine.storage.MMapFileChannel;
import com.hunkyhsu.minidb.engine.storage.PageLayout;
import com.hunkyhsu.minidb.engine.storage.TuplePointer;
import com.hunkyhsu.minidb.engine.transaction.TransactionContext;

/**
 * @author hunkyhsu
 * @version 1.0
 * @date 2026/4/5 21:12
 */
public class SeqScanNode implements DbIterator {
    private final MMapFileChannel channel;
    private final AppendOnlyTableStore store;
    private final TransactionContext txContext;

    private int currentOffset;
    private int endOffset;

    public SeqScanNode(MMapFileChannel channel, AppendOnlyTableStore store,  TransactionContext txContext) {
        this.channel = channel;
        this.store = store;
        this.txContext = txContext;
    }

    public void open() {
        currentOffset = 0;
        endOffset = store.getValidEndOffset();
    }

    public long next() {
        while (currentOffset < endOffset) {
            int pageId = currentOffset / PageLayout.PAGE_SIZE;
            int pageOffset = currentOffset % PageLayout.PAGE_SIZE;
            long tuplePointer = TuplePointer.pack(pageId, pageOffset);
            long currentXmin = channel.readXmin(tuplePointer);
            // Is empty space, need to next page
            if (currentXmin == 0) {
                currentOffset = (pageId + 1) * PageLayout.PAGE_SIZE;
            } else {
                int payloadLength = channel.readPayloadLength(tuplePointer);
                currentOffset += PageLayout.HEADER_SIZE + payloadLength;
                if (txContext.isVisible(currentXmin)) {
                    return tuplePointer;
                }
            }
        }
        return DbIterator.EOF;
    }

    public void close() {
        currentOffset = -1;
    }
}
