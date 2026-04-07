package com.hunkyhsu.minidb.api;

import com.hunkyhsu.minidb.dto.InsertRequest;
import com.hunkyhsu.minidb.dto.RowDto;
import com.hunkyhsu.minidb.dto.SelectRequest;
import com.hunkyhsu.minidb.engine.catalog.CatalogManager;
import com.hunkyhsu.minidb.engine.catalog.Column;
import com.hunkyhsu.minidb.engine.catalog.Type;
import com.hunkyhsu.minidb.engine.execution.DbIterator;
import com.hunkyhsu.minidb.engine.execution.FilterNode;
import com.hunkyhsu.minidb.engine.execution.SeqScanNode;
import com.hunkyhsu.minidb.engine.storage.AppendOnlyTableStore;
import com.hunkyhsu.minidb.engine.storage.MMapFileChannel;
import com.hunkyhsu.minidb.engine.transaction.TransactionContext;
import com.hunkyhsu.minidb.engine.transaction.TransactionManager;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

@RestController
@RequestMapping("/api")
public class SystemController {
    private final TransactionManager txManager;
    private final AppendOnlyTableStore store;
    private final MMapFileChannel channel;
    private final CatalogManager catalogManager;

    public SystemController(TransactionManager txManager,
                            AppendOnlyTableStore store,
                            MMapFileChannel channel,
                            CatalogManager catalogManager) {
        this.txManager = txManager;
        this.store = store;
        this.channel = channel;
        this.catalogManager = catalogManager;

        List<Column> columns = List.of(
                new Column("id", Type.INT, 0), // 最后的 0 是占位符，TableMetadata 会重新推导
                new Column("age", Type.INT, 0)
        );
        this.catalogManager.createTable("users", columns);
    }

    @PostMapping("/insert")
    public String insert(@RequestBody InsertRequest request) {
        long xmin = txManager.beginWriteTransaction();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(request.id());
        buffer.putInt(request.age());
        store.insertTuple(xmin, buffer.array());
        txManager.commitTransaction(xmin);
        return "Commit Success";
    }

    @PostMapping("/select")
    public List<RowDto> select(@RequestBody SelectRequest request) {
        TransactionContext snapshot = txManager.beginReadSnapshot(9999);

        Predicate<Long> ageFilter = pointer -> {
            byte[] payload = channel.readPayload(pointer);
            int age = ByteBuffer.wrap(payload).getInt(catalogManager.getTable("users").getColumnOffset("age"));
            return age >= request.filterAge();
        };

        SeqScanNode scanNode = new SeqScanNode(channel, store, snapshot);
        FilterNode filterNode = new FilterNode(scanNode, ageFilter);
        filterNode.open();
        List<RowDto> rows = new ArrayList<>();
        long currentPtr;
        while ((currentPtr = filterNode.next()) != DbIterator.EOF) {
            byte[] payload = channel.readPayload(currentPtr);
            int id = ByteBuffer.wrap(payload).getInt();
            int age = ByteBuffer.wrap(payload).getInt(4);
            RowDto row = new RowDto(id, age);
            rows.add(row);
        }
        filterNode.close();

        return rows;
    }

}
