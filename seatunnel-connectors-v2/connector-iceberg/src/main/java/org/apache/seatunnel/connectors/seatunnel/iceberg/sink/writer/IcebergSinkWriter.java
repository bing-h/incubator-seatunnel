package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DataConverter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDataConverter;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.ArrayUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class IcebergSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private SinkWriter.Context context;

    private Schema tableSchema;

    private SeaTunnelRowType seaTunnelRowType;
    private IcebergTableLoader icebergTableLoader;
    private SinkConfig sinkConfig;
    private Table table;

    private List<Record> pendingRows = new ArrayList<>();
    private DataConverter defaultDataConverter;

    private static final int FORMAT_V2 = 2;

    private final FileFormat format;

    private PartitionKey partition = null;
    private OutputFileFactory fileFactory = null;

    public IcebergSinkWriter(
            SinkWriter.Context context,
            Schema tableSchema,
            SeaTunnelRowType seaTunnelRowType,
            SinkConfig sinkConfig) {
        this.context = context;
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.seaTunnelRowType = seaTunnelRowType;
        defaultDataConverter = new DefaultDataConverter(seaTunnelRowType, tableSchema);
        if (icebergTableLoader == null) {
            icebergTableLoader = IcebergTableLoader.create(sinkConfig);
            icebergTableLoader.open();
        }
        if (table == null) {
            table = icebergTableLoader.loadTable();
        }
        this.format = FileFormat.valueOf(sinkConfig.getFileFormat().toUpperCase(Locale.ENGLISH));
        this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
        this.partition = createPartitionKey();
    }

    private PartitionKey createPartitionKey() {
        if (table.spec().isUnpartitioned()) {
            return null;
        }
        Record record = GenericRecord.create(table.schema());
        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        partitionKey.partition(record);
        return partitionKey;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        pendingRows.add(defaultDataConverter.toIcebergStruct(element));
        if (pendingRows.size() >= sinkConfig.getMaxRow()) {
            FileAppenderFactory<Record> appenderFactory = createAppenderFactory(null, null, null);
            DataFile dataFile = prepareDataFile(pendingRows, appenderFactory);
            table.newRowDelta().addRows(dataFile).commit();
            pendingRows.clear();
        }
    }

    private DataFile prepareDataFile(
            List<Record> rowSet, FileAppenderFactory<Record> appenderFactory) throws IOException {
        DataWriter<Record> writer =
                appenderFactory.newDataWriter(createEncryptedOutputFile(), format, partition);
        try (DataWriter<Record> closeableWriter = writer) {
            for (Record row : rowSet) {
                closeableWriter.write(row);
            }
        }
        return writer.toDataFile();
    }

    private EncryptedOutputFile createEncryptedOutputFile() {
        if (partition == null) {
            return fileFactory.newOutputFile();
        } else {
            return fileFactory.newOutputFile(partition);
        }
    }

    protected FileAppenderFactory<Record> createAppenderFactory(
            List<Integer> equalityFieldIds, Schema eqDeleteSchema, Schema posDeleteRowSchema) {
        return new GenericAppenderFactory(
                table.schema(),
                table.spec(),
                ArrayUtil.toIntArray(equalityFieldIds),
                eqDeleteSchema,
                posDeleteRowSchema);
    }

    @Override
    public void close() throws IOException {
        if (icebergTableLoader != null) {
            icebergTableLoader.close();
            pendingRows.clear();
        }
    }
}
