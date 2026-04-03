package com.hunkyhsu.minidb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Disk IO Manager
 */
public class DiskManager implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

    private static final ByteBuffer EMPTY_PAGE_BUFFER;

    static {
        // 初始化空 Page Buffer（全 0）
        EMPTY_PAGE_BUFFER = ByteBuffer.allocateDirect(Page.PAGE_SIZE);
    }

    // 单文件最大页面数（2GB / 4KB = 524288 pages）
    private static final int MAX_PAGES_PER_FILE = 524288;
    // Extent 大小（一次分配 8 个页面）
    private static final int EXTENT_SIZE = 8;

    private final String dbFileBasePath;
    private final ConcurrentHashMap<Integer, FileChannel> fileChannels;
    private final AtomicInteger numPages;
    private final Set<Integer> freePages;
    private final LinkedBlockingQueue<Integer> preallocatedPages;

    public DiskManager(String dbFilePath) throws IOException {
        this.dbFileBasePath = dbFilePath;
        this.fileChannels = new ConcurrentHashMap<>();
        this.freePages = ConcurrentHashMap.newKeySet();
        this.preallocatedPages = new LinkedBlockingQueue<>();

        // 确保父目录存在
        Path basePath = Path.of(dbFilePath);
        Path parentPath = basePath.getParent();
        if (parentPath != null) {
            File parentDir = parentPath.toFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
            }
        }

        // 扫描现有文件并初始化
        int totalPages = 0;
        int fileIndex = 0;
        while (true) {
            String filePath = getFilePathForIndex(fileIndex);
            File file = new File(filePath);
            if (!file.exists()) {
                break;
            }

            FileChannel channel = FileChannel.open(
                    Path.of(filePath),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );
            fileChannels.put(fileIndex, channel);

            long fileSize = channel.size();
            if (fileSize % Page.PAGE_SIZE != 0) {
                logger.warn("File {} size {} is not a multiple of PAGE_SIZE {}, file may be corrupted",
                        filePath, fileSize, Page.PAGE_SIZE);
            }
            totalPages += (int) (fileSize / Page.PAGE_SIZE);
            fileIndex++;
        }

        // 如果没有文件，创建第一个文件
        if (fileIndex == 0) {
            FileChannel channel = FileChannel.open(
                    Path.of(getFilePathForIndex(0)),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );
            fileChannels.put(0, channel);
        }

        this.numPages = new AtomicInteger(totalPages);
        logger.info("DiskManager initialized: {} files, {} pages", fileChannels.size(), totalPages);
    }

    /**
     * 根据文件索引获取文件路径
     */
    private String getFilePathForIndex(int fileIndex) {
        if (fileIndex == 0) {
            return dbFileBasePath;
        }
        return dbFileBasePath + "." + fileIndex;
    }

    /**
     * 根据 pageId 获取对应的文件索引
     */
    private int getFileIndexForPage(int pageId) {
        return pageId / MAX_PAGES_PER_FILE;
    }

    /**
     * 根据 pageId 获取在文件内的偏移页号
     */
    private int getPageOffsetInFile(int pageId) {
        return pageId % MAX_PAGES_PER_FILE;
    }

    /**
     * 获取或创建指定索引的 FileChannel
     */
    private FileChannel getOrCreateFileChannel(int fileIndex) throws IOException {
        return fileChannels.computeIfAbsent(fileIndex, idx -> {
            try {
                String filePath = getFilePathForIndex(idx);
                FileChannel channel = FileChannel.open(
                        Path.of(filePath),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                );
                logger.info("Created new database file: {}", filePath);
                return channel;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create file channel for index " + idx, e);
            }
        });
    }

    public void readPage(int pageId, Page page) throws IOException {
        if (pageId < 0 || pageId >= numPages.get()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid pageId: %d (total pages: %d)", pageId, numPages.get()));
        }

        int fileIndex = getFileIndexForPage(pageId);
        int pageOffset = getPageOffsetInFile(pageId);
        long offset = (long) pageOffset * Page.PAGE_SIZE;

        FileChannel channel = getOrCreateFileChannel(fileIndex);
        ByteBuffer buffer = page.getPageData();
        buffer.clear();

        int totalBytesRead = 0;
        while (totalBytesRead < Page.PAGE_SIZE) {
            int bytesRead = channel.read(buffer, offset + totalBytesRead);
            if (bytesRead == -1) {
                throw new IOException(String.format(
                        "Unexpected EOF: page %d is incomplete (expected %d bytes, got %d)",
                        pageId, Page.PAGE_SIZE, totalBytesRead));
            }
            totalBytesRead += bytesRead;
        }

        buffer.flip();
        if (logger.isDebugEnabled()) {
            logger.debug("Read page {} from file {} (offset={}, bytes={})",
                    pageId, fileIndex, offset, totalBytesRead);
        }
    }

    public void writePage(int pageId, Page page) throws IOException {
        if (pageId < 0 || pageId >= numPages.get()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid pageId: %d (total pages: %d)", pageId, numPages.get()));
        }

        int fileIndex = getFileIndexForPage(pageId);
        int pageOffset = getPageOffsetInFile(pageId);
        long offset = (long) pageOffset * Page.PAGE_SIZE;

        try {
            FileChannel channel = getOrCreateFileChannel(fileIndex);
            ByteBuffer buffer = page.getPageData();
            buffer.rewind();

            int totalBytesWritten = 0;
            while (buffer.hasRemaining()) {
                int written = channel.write(buffer, offset + totalBytesWritten);
                totalBytesWritten += written;
            }
            channel.force(false);

            if (logger.isDebugEnabled()) {
                logger.debug("Wrote page {} to file {} (offset={}, bytes={})",
                        pageId, fileIndex, offset, totalBytesWritten);
            }
        } catch (IOException e) {
            throw new IOException(String.format(
                    "Failed to write page %d: %s", pageId, e.getMessage()), e
            );
        }
    }

    /**
     * 分配新页面（支持 Extent-Based Allocation 和 FreePageList）
     *
     * 分配策略：
     * 1. 优先从 FreePageList 重用已释放的页面
     * 2. 其次从预分配的 Extent 中获取页面
     * 3. 最后分配新的 Extent（一次分配 8 个页面）
     *
     * @return 新分配的页面ID
     * @throws IOException 磁盘空间不足或I/O错误
     */
    public int allocatePage() throws IOException {
        // 策略 1: 优先从 FreePageList 重用
        Integer freePage = freePages.stream().findFirst().orElse(null);
        if (freePage != null) {
            freePages.remove(freePage);
            logger.debug("Reused free page {} from FreePageList", freePage);
            return freePage;
        }

        // 策略 2: 从预分配的 Extent 中获取
        Integer preallocatedPage = preallocatedPages.poll();
        if (preallocatedPage != null) {
            logger.debug("Allocated page {} from preallocated extent", preallocatedPage);
            return preallocatedPage;
        }

        // 策略 3: 分配新的 Extent（一次分配 8 个页面）
        allocateExtent();

        // 从新分配的 Extent 中获取第一个页面
        preallocatedPage = preallocatedPages.poll();
        if (preallocatedPage == null) {
            throw new IOException("Failed to allocate extent");
        }

        logger.debug("Allocated page {} from new extent", preallocatedPage);
        return preallocatedPage;
    }

    /**
     * 分配一个 Extent（8 个连续页面）
     *
     * 优势：
     * - 减少系统调用次数（从 8 次减少到 1 次）
     * - 提高顺序写入性能
     * - 减少文件碎片
     */
    private void allocateExtent() throws IOException {
        int startPageId = numPages.getAndAdd(EXTENT_SIZE);
        int fileIndex = getFileIndexForPage(startPageId);
        int pageOffset = getPageOffsetInFile(startPageId);
        long offset = (long) pageOffset * Page.PAGE_SIZE;

        // 检查是否需要创建新文件
        if (pageOffset + EXTENT_SIZE > MAX_PAGES_PER_FILE) {
            // 跨文件边界，分别处理
            int pagesInCurrentFile = MAX_PAGES_PER_FILE - pageOffset;
            allocateExtentInFile(fileIndex, offset, pagesInCurrentFile, startPageId);
            allocateExtentInFile(fileIndex + 1, 0, EXTENT_SIZE - pagesInCurrentFile,
                               startPageId + pagesInCurrentFile);
        } else {
            // 在同一文件内分配
            allocateExtentInFile(fileIndex, offset, EXTENT_SIZE, startPageId);
        }

        logger.info("Allocated extent: pages {}-{} (total pages: {})",
                   startPageId, startPageId + EXTENT_SIZE - 1, numPages.get());
    }

    /**
     * 在指定文件中分配页面
     */
    private void allocateExtentInFile(int fileIndex, long offset, int numPagesToAllocate,
                                     int startPageId) throws IOException {
        FileChannel channel = getOrCreateFileChannel(fileIndex);

        // 一次性写入多个空页面
        int totalSize = numPagesToAllocate * Page.PAGE_SIZE;
        ByteBuffer buffer = ByteBuffer.allocateDirect(totalSize);

        int totalWritten = 0;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, offset + totalWritten);
            if (written == 0) {
                throw new IOException("Cannot write to disk, possibly full");
            }
            totalWritten += written;
        }
        channel.force(false);

        // 将新分配的页面加入预分配队列
        for (int i = 0; i < numPagesToAllocate; i++) {
            preallocatedPages.offer(startPageId + i);
        }

        logger.debug("Allocated {} pages in file {} at offset {}",
                    numPagesToAllocate, fileIndex, offset);
    }

    /**
     * 释放页面（加入 FreePageList）
     *
     * @param pageId 要释放的页面ID
     */
    public void deallocatePage(int pageId) {
        if (pageId < 0 || pageId >= numPages.get()) {
            logger.warn("Attempted to deallocate invalid pageId: {}", pageId);
            return;
        }
        freePages.add(pageId);
        logger.debug("Deallocated page {} (added to FreePageList, size={})",
                    pageId, freePages.size());
    }

    /**
     * 获取空闲页面数量
     */
    public int getFreePagesCount() {
        return freePages.size();
    }

    /**
     * 获取预分配页面数量
     */
    public int getPreallocatedPagesCount() {
        return preallocatedPages.size();
    }

    public int getNumPages() {
        return numPages.get();
    }

    public String getDbFilePath() {
        return dbFileBasePath;
    }

    public long getFileSize() throws IOException {
        long totalSize = 0;
        for (FileChannel channel : fileChannels.values()) {
            totalSize += channel.size();
        }
        return totalSize;
    }

    /**
     * 获取文件数量
     */
    public int getFileCount() {
        return fileChannels.size();
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format(
            "DiskManager Stats: files=%d, totalPages=%d, freePages=%d, preallocatedPages=%d",
            fileChannels.size(), numPages.get(), freePages.size(), preallocatedPages.size()
        );
    }

    @Override
    public void close() {
        try {
            // 关闭所有文件通道
            for (Map.Entry<Integer, FileChannel> entry : fileChannels.entrySet()) {
                FileChannel channel = entry.getValue();
                if (channel != null && channel.isOpen()) {
                    channel.force(true);
                    channel.close();
                    logger.debug("Closed file channel for index {}", entry.getKey());
                }
            }
            logger.info("DiskManager closed: {} files, {} pages, {} free pages",
                       fileChannels.size(), numPages.get(), freePages.size());
        } catch (IOException e) {
            logger.error("Failed to close DiskManager", e);
        }
    }
}
