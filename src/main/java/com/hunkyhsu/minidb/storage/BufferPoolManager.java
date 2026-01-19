package com.hunkyhsu.minidb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPoolManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BufferPoolManager.class);

    private final int poolSize;

    private final Page[] pages;

    private final ConcurrentHashMap<Integer, Integer> pageTable;

    private final LinkedBlockingQueue<Integer> freeList;

    private final Replacer replacer;

    private final DiskManager diskManager;

    private final ReentrantLock globalLock;

    public BufferPoolManager(int poolSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.pages = new Page[poolSize];
        this.pageTable = new ConcurrentHashMap<>(poolSize);
        this.freeList = new LinkedBlockingQueue<>(poolSize);
        this.replacer = new LRUReplacer(poolSize);
        this.diskManager = diskManager;
        this.globalLock = new ReentrantLock();

        for (int i = 0; i < poolSize; i++) {
            pages[i] = new Page();
            freeList.offer(i);
        }

        logger.info("BufferPoolManager initialized: poolSize={}", poolSize);
    }

    // Get page (auto-pin)
    public Page fetchPage(int pageId) throws IOException {
        globalLock.lock();
        try {
            // case 1. 检查 Page 是否在内存中
            if (pageTable.containsKey(pageId)) {
                int frameId = pageTable.get(pageId);
                Page page = pages[frameId];
                page.pin();
                replacer.pin(frameId);
                // 重置 ByteBuffer position 到开头，方便用户读取
                page.getPageData().rewind();
                logger.debug("Page {} hit in buffer pool (frameId={})", pageId, frameId);
                return page;
            }
            // case 2. Page 不在内存，需要从磁盘加载
            int frameId = findVictimFrame();
            if (frameId == -1) {
                throw new IOException("All pages are pinned, cannot allocate frame");
            }
            // current page is old victim page
            Page page = pages[frameId];
            if (page != null) {
                if (page.isDirty()) {
                    diskManager.writePage(page.getPageId(), page);
                    logger.debug("Flushed dirty page {} before eviction", page.getPageId());
                }
                pageTable.remove(page.getPageId());
            }
            page.resetMemory();
            // current page is new empty page
            page.setPageId(pageId);
            diskManager.readPage(pageId, page);

            pageTable.put(pageId, frameId);
            page.pin();
            replacer.pin(frameId);
            logger.debug("Page {} loaded from disk (frameId={})", pageId, frameId);
            return page;
        } finally {
            globalLock.unlock();
        }
    }

    // release page
    public void unpinPage(int pageId, boolean isDirty) {
        globalLock.lock();
        try {
            if (!pageTable.containsKey(pageId)) {
                logger.warn("Attempted to unpin non-existent page {}", pageId);
                return;
            }
            int frameId = pageTable.get(pageId);
            Page page = pages[frameId];
            if (isDirty) {
                page.setDirty(true);
            }
            page.unpin();
            if (page.getPinCount() == 0) {
                replacer.unpin(frameId);
                logger.debug("Page {} unpinned (frameId={}, pinCount=0, added to LRU)",
                        pageId, frameId);
            } else {
                logger.debug("Page {} unpinned (frameId={}, pinCount={})",
                        pageId, frameId, page.getPinCount());
            }

        } finally {
            globalLock.unlock();
        }
    }

    // Manual Flush: 1. CheckPoint; 2. Database close; 3. Testing
    public boolean flushPage(int pageId) throws IOException {
        globalLock.lock();
        try {
            if (!pageTable.containsKey(pageId)) {
                logger.warn("Attempted to flush non-existent page {}", pageId);
                return false;
            }

            int frameId = pageTable.get(pageId);
            Page page = pages[frameId];
            diskManager.writePage(pageId, page);
            page.setDirty(false);

            logger.debug("Page {} flushed to disk", pageId);
            return true;

        } finally {
            globalLock.unlock();
        }
    }

    public void flushAllPages() throws IOException {
        globalLock.lock();
        try {
            for (int pageId : pageTable.keySet()) {
                flushPage(pageId);
            }
            logger.info("Flushed {} dirty pages to disk", pageTable.keySet());
        } finally {
            globalLock.unlock();
        }
    }

    public Page newPage() throws IOException {
        globalLock.lock();
        try {
            // 1. 分配新 Page ID
            int newPageId = diskManager.allocatePage();

            // 2. 找一个可用的 Frame
            int frameId = findVictimFrame();
            if (frameId == -1) {
                // 分配失败，回滚磁盘分配
                // TODO: 实现 deallocatePage 时取消注释
                // diskManager.deallocatePage(newPageId);
                throw new IOException("All pages are pinned, cannot create new page");
            }

            // 3. 如果 Frame 中有旧 Page，处理淘汰逻辑
            Page oldPage = pages[frameId];
            if (oldPage != null) {
                int oldPageId = oldPage.getPageId();
                if (oldPage.isDirty()) {
                    diskManager.writePage(oldPageId, oldPage);
                }
                pageTable.remove(oldPageId);
            }
            oldPage.resetMemory();
            oldPage.setPageId(newPageId);
            pageTable.put(newPageId, frameId);
            oldPage.pin();
            replacer.pin(frameId);

            logger.info("Created new page {} (frameId={})", newPageId, frameId);
            return oldPage;

        } finally {
            globalLock.unlock();
        }
    }

    public boolean deletePage(int pageId) {
        globalLock.lock();
        try {
            if (!pageTable.containsKey(pageId)) {
                logger.warn("Attempted to delete non-existent page {}", pageId);
                return false;
            }

            int frameId = pageTable.get(pageId);
            Page page = pages[frameId];

            // 如果 Page 正在使用，不能删除
            if (page.getPinCount() > 0) {
                logger.warn("Cannot delete page {} (pinCount={})", pageId, page.getPinCount());
                return false;
            }
            // 从 PageTable 和 Replacer 中移除
            pageTable.remove(pageId);
            replacer.pin(frameId);
            page.resetMemory();
            page.setPageId(-1);
            freeList.offer(frameId);

            logger.info("Deleted page {} (frameId={})", pageId, frameId);
            return true;

        } finally {
            globalLock.unlock();
        }
    }

    private int findVictimFrame() {
        // 1. 优先从空闲列表获取
        Integer frameId = freeList.poll();
        if (frameId != null) {
            logger.trace("Allocated free frame {}", frameId);
            return frameId;
        }

        // 2. 没有空闲 Frame，通过 LRU 淘汰
        frameId = replacer.victim();
        if (frameId != -1) {
            logger.trace("Evicted frame {} via LRU", frameId);
            return frameId;
        }

        // 3. 所有 Page 都被 pin 住，无法淘汰
        logger.warn("No victim frame available (all pages are pinned)");
        return -1;
    }

    public String getStats() {
        globalLock.lock();
        try {
            int usedFrames = poolSize - freeList.size();
            int dirtyPages = 0;
            int pinnedPages = 0;

            for (Page page : pages) {
                if (page != null) {
                    if (page.isDirty()) dirtyPages++;
                    if (page.getPinCount() > 0) pinnedPages++;
                }
            }

            return String.format(
                    "BufferPool Stats: poolSize=%d, used=%d, free=%d, dirty=%d, pinned=%d, evictable=%d",
                    poolSize, usedFrames, freeList.size(), dirtyPages, pinnedPages, replacer.size()
            );

        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        globalLock.lock();
        try {
            logger.info("Closing BufferPoolManager...");
            flushAllPages();
            pageTable.clear();
            freeList.clear();
            logger.info("BufferPoolManager closed. {}", getStats());

        } finally {
            globalLock.unlock();
        }
    }
}