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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Disk IO Manager
 */
public class DiskManager implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

    /**
     * 静态空 Page Buffer（全局共享，零 GC）
     * 设计决策：
     * - 使用 DirectBuffer 以获得更好的 I/O 性能
     * - 声明为 static final 避免重复分配
     * - 通过 slice() 创建线程安全的独立视图
     */
    private static final ByteBuffer EMPTY_PAGE_BUFFER;

    static {
        // 初始化空 Page Buffer（全 0）
        EMPTY_PAGE_BUFFER = ByteBuffer.allocateDirect(Page.PAGE_SIZE);
        // DirectBuffer 默认就是全 0，无需额外填充
        logger.info("Initialized static EMPTY_PAGE_BUFFER ({} bytes)", Page.PAGE_SIZE);
    }

    private final FileChannel fileChannel;
    private final Path dbFilePath;
    private final AtomicInteger numPages;

    public DiskManager(String dbFilePath) throws IOException {
        this.dbFilePath = Path.of(dbFilePath);

        // 确保父目录存在（如果有父目录）
        Path parentPath = this.dbFilePath.getParent();
        if (parentPath != null) {
            File parentDir = parentPath.toFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
            }
        }
        this.fileChannel = FileChannel.open(
                this.dbFilePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        long fileSize = this.fileChannel.size();
        if (fileSize % Page.PAGE_SIZE != 0) {
            logger.warn("File size {} is not a multiple of PAGE_SIZE {}, file may be corrupted",
                    fileSize, Page.PAGE_SIZE);
        }
        this.numPages = new AtomicInteger((int) (fileSize / Page.PAGE_SIZE));
    }

    public void readPage(int pageId, Page page) throws IOException {
        if (pageId < 0 || pageId >= numPages.get()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid pageId: %d (total pages: %d)", pageId, numPages.get()));
        }
        long offset = (long) pageId * Page.PAGE_SIZE;

        ByteBuffer buffer = page.getPageData();
        buffer.clear();

        int totalBytesRead = 0;
        while (totalBytesRead < Page.PAGE_SIZE) {
            int bytesRead = this.fileChannel.read(buffer, offset + totalBytesRead);
            if (bytesRead == -1) {
                throw new IOException(String.format(
                        "Unexpected EOF: page %d is incomplete (expected %d bytes, got %d)",
                        pageId, Page.PAGE_SIZE, totalBytesRead));
            }
            totalBytesRead += bytesRead;
            if (logger.isDebugEnabled()) {
                logger.debug("Read page {} from disk (offset={}, bytes={})",
                        pageId, offset, totalBytesRead);
            }
        }

        // 读取完成后，flip() 准备读取数据
        // flip() 将 position 设为 0，limit 设为当前 position（即已读取的字节数）
        buffer.flip();
    }

    public void writePage(int pageId, Page page) throws IOException {
        if (pageId < 0 || pageId >= numPages.get()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid pageId: %d (total pages: %d)", pageId, numPages.get()));
        }
        long offset = (long) pageId * Page.PAGE_SIZE;
        try {
            ByteBuffer buffer = page.getPageData();
            buffer.rewind();  // 重置到开头
            int totalBytesWritten = 0;
            while (buffer.hasRemaining()) {
                int written = fileChannel.write(buffer, offset + totalBytesWritten);
                totalBytesWritten += written;
            }
            fileChannel.force(false);
            if (logger.isDebugEnabled()) {
                logger.debug("Wrote page {} to disk (offset={}, bytes={})",
                        pageId, offset, totalBytesWritten);
            }
        } catch (IOException e) {
            throw new IOException(String.format(
                    "Failed to write page %d (offset=%d): %s", pageId, offset, e.getMessage()), e
            );
        }
    }

    // TODO: Upgrade to Extent-Based Allocation
    public int allocatePage() throws IOException {
        int newPageId = this.numPages.getAndIncrement();
        long offset = (long) newPageId * Page.PAGE_SIZE;

        ByteBuffer buffer = EMPTY_PAGE_BUFFER.slice();
        try {
            while (buffer.hasRemaining()) {
                int written = fileChannel.write(buffer, offset + buffer.position());
                if (written == 0) {
                    throw new IOException("Cannot write to disk, possibly full");
                }
            }
            fileChannel.force(false);
            if (logger.isDebugEnabled()) {
                logger.debug("Allocated new page {} (total pages: {})", newPageId, numPages.get());
            }
            return newPageId;
        } catch (IOException e) {
            numPages.decrementAndGet();
            logger.error("Failed to allocate page {}, rolling back numPages to {}",
                    newPageId, numPages.get());
            throw new IOException(
                    String.format("Failed to allocate page %d: %s", newPageId, e.getMessage()),
                    e
            );
        }
    }

    public void deallocatePage(int pageId) {
        if (pageId < 0 || pageId >= numPages.get()) {
            logger.warn("Attempted to deallocate invalid pageId: {}", pageId);
            return;
        }

        // TODO: 阶段二实现 FreePageList，将 pageId 加入空闲列表
        logger.debug("Deallocated page {} (not yet implemented)", pageId);
    }

    public int getNumPages() {
        return numPages.get();
    }

    public String getDbFilePath() {
        return dbFilePath.toAbsolutePath().toString();
    }

    public long getFileSize() throws IOException {
        return fileChannel.size();
    }

    @Override
    public void close() {
        try {
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.force(true);
                fileChannel.close();
                logger.info("Disk Manager closed: file={}", dbFilePath.toAbsolutePath());
            }
        } catch (IOException e) {
            logger.error("Failed to close Disk Manager", e);
        }
    }

}
