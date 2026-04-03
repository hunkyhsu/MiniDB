package com.hunkyhsu.minidb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU Replacer - 基于 LRU 算法的页面替换器
 *
 * 核心设计：
 * - 使用 LinkedHashMap（accessOrder=true）实现 LRU
 * - 线程安全：所有操作加锁保护
 * - 只管理 pinCount == 0 的 Frame（可淘汰的 Frame）
 *
 * 数据结构：
 * - LinkedHashMap 的迭代顺序 = 访问顺序（最近访问的在末尾）
 * - 最久未访问的 Frame 在头部，最先被淘汰
 *
 * @author hunkyhsu
 */
public class LRUReplacer implements Replacer {

    private static final Logger logger = LoggerFactory.getLogger(LRUReplacer.class);

    private final LinkedHashMap<Integer, Boolean> lruMap;

    private final ReentrantLock lock;

    public LRUReplacer(int capacity) {
        this.lruMap = new LinkedHashMap<>(capacity, 0.75f, true);
        this.lock = new ReentrantLock();
        logger.info("LRU Replacer initialized with capacity {}", capacity);
    }

    /**
     * Get 1st frame from LinkedHashMap (LRU) and victim it
     * @return Frame ID or -1 if no victim
     */
    @Override
    public int victim() {
        lock.lock();
        try {
            Iterator<Map.Entry<Integer, Boolean>> it = lruMap.entrySet().iterator();
            if (it.hasNext()) {
                int frameId = it.next().getKey();
                it.remove();
                logger.debug("Victim selected: frameId={}", frameId);
                return frameId;
            }
            logger.debug("No victim available (all frames are pinned)");
            return -1;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Mark the frame as non-removable and
     * 将 Frame 标记为不可淘汰（被 pin 住）
     *
     * 实现：从 LRU 列表中移除
     *
     * @param frameId Frame ID
     */
    @Override
    public void pin(int frameId) {
        lock.lock();
        try {
            lruMap.remove(frameId);
            logger.trace("Frame {} pinned (removed from LRU)", frameId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将 Frame 标记为可淘汰（被 unpin）
     *
     * 实现：加入 LRU 列表
     *
     * @param frameId Frame ID
     */
    @Override
    public void unpin(int frameId) {
        lock.lock();
        try {
            lruMap.put(frameId, true);
            logger.trace("Frame {} unpinned (added to LRU)", frameId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取当前可淘汰的 Frame 数量
     *
     * @return 可淘汰的 Frame 数量
     */
    @Override
    public int size() {
        lock.lock();
        try {
            return lruMap.size();
        } finally {
            lock.unlock();
        }
    }
}