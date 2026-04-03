package com.hunkyhsu.minidb.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DiskManager tests
 *
 * Coverage goals:
 * - allocate/read/write/deallocate
 * - free list + preallocated queue
 * - stats + counts + file size
 * - invalid page id handling
 * - persistence across reopen
 */
class DiskManagerTest {

    private static final String TEST_DB_PATH = "test_disk_manager.db";

    private DiskManager diskManager;

    @BeforeEach
    void setUp() throws IOException {
        deleteTestFiles();
        diskManager = new DiskManager(TEST_DB_PATH);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (diskManager != null) {
            diskManager.close();
        }
        deleteTestFiles();
    }

    private void deleteTestFiles() throws IOException {
        Path base = Path.of(TEST_DB_PATH).toAbsolutePath();
        Path dir = base.getParent();
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        String baseName = base.getFileName().toString();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(p -> {
                String name = p.getFileName().toString();
                return name.equals(baseName) || name.startsWith(baseName + ".");
            }).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    @Test
    @DisplayName("allocate/read/write + stats + counts")
    void testAllocateReadWriteAndStats() throws IOException {
        int pageId = diskManager.allocatePage();
        assertEquals(0, pageId, "First page should be 0");

        // extent allocation should create 8 pages
        assertEquals(8, diskManager.getNumPages());
        assertEquals(7, diskManager.getPreallocatedPagesCount());
        assertEquals(0, diskManager.getFreePagesCount());
        assertEquals(1, diskManager.getFileCount());
        assertEquals(TEST_DB_PATH, diskManager.getDbFilePath());

        Page writePage = new Page();
        writePage.setPageId(pageId);
        byte[] data = "hello".getBytes();
        writePage.getPageData().put(data);
        diskManager.writePage(pageId, writePage);

        Page readPage = new Page();
        diskManager.readPage(pageId, readPage);
        byte[] readData = new byte[data.length];
        readPage.getPageData().get(readData);
        assertArrayEquals(data, readData);

        assertEquals(8L * Page.PAGE_SIZE, diskManager.getFileSize());
        assertTrue(diskManager.getStats().contains("DiskManager Stats"));
    }

    @Test
    @DisplayName("free list reuse + deallocate")
    void testFreeListReuse() throws IOException {
        int pageId = diskManager.allocatePage();
        diskManager.deallocatePage(pageId);
        assertEquals(1, diskManager.getFreePagesCount());

        int reused = diskManager.allocatePage();
        assertEquals(pageId, reused, "Should reuse freed page");
        assertEquals(0, diskManager.getFreePagesCount());
    }

    @Test
    @DisplayName("extent allocation + preallocated queue")
    void testExtentAllocationAndPreallocatedQueue() throws IOException {
        for (int i = 0; i < 8; i++) {
            assertEquals(i, diskManager.allocatePage());
        }
        assertEquals(8, diskManager.getNumPages());
        assertEquals(0, diskManager.getPreallocatedPagesCount());

        int page8 = diskManager.allocatePage();
        assertEquals(8, page8, "First page of new extent");
        assertEquals(16, diskManager.getNumPages());
        assertEquals(7, diskManager.getPreallocatedPagesCount());
    }

    @Test
    @DisplayName("invalid page id handling")
    void testInvalidPageIds() throws IOException {
        diskManager.allocatePage();

        Page page = new Page();
        assertThrows(IllegalArgumentException.class, () -> diskManager.readPage(-1, page));
        assertThrows(IllegalArgumentException.class, () -> diskManager.readPage(999, page));
        assertThrows(IllegalArgumentException.class, () -> diskManager.writePage(-1, page));
        assertThrows(IllegalArgumentException.class, () -> diskManager.writePage(999, page));

        int freeBefore = diskManager.getFreePagesCount();
        diskManager.deallocatePage(-1);
        diskManager.deallocatePage(999);
        assertEquals(freeBefore, diskManager.getFreePagesCount());
    }

    @Test
    @DisplayName("persistence across reopen")
    void testPersistenceAcrossReopen() throws IOException {
        int pageId = diskManager.allocatePage();
        Page writePage = new Page();
        writePage.setPageId(pageId);
        byte[] data = "persist".getBytes();
        writePage.getPageData().put(data);
        diskManager.writePage(pageId, writePage);

        diskManager.close();
        diskManager = null;

        diskManager = new DiskManager(TEST_DB_PATH);
        Page readPage = new Page();
        diskManager.readPage(pageId, readPage);
        byte[] readData = new byte[data.length];
        readPage.getPageData().get(readData);
        assertArrayEquals(data, readData);
        assertEquals(8, diskManager.getNumPages());
    }
}
