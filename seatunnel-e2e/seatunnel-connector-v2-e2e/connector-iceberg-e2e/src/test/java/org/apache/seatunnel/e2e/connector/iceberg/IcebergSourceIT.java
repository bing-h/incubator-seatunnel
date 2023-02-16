/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.iceberg;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.GLUE;

import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class IcebergSourceIT {

    private Table table;

    private static final int FORMAT_V2 = 2;

    private  FileFormat format;

    private PartitionKey partition = null;
    private OutputFileFactory fileFactory = null;

    public IcebergSourceIT(){
    }

    public static  void main(String[] args) throws IOException {
        IcebergSourceIT icebergSourceIT = new IcebergSourceIT();
        icebergSourceIT.start();
    }

    private static final TableIdentifier TABLE = TableIdentifier.of(
        Namespace.of("database1"), "source");
    private static final Schema SCHEMA = new Schema(
        Types.NestedField.optional(1, "f1", Types.LongType.get()),
        Types.NestedField.optional(2, "f2", Types.BooleanType.get()),
        Types.NestedField.optional(3, "f3", Types.IntegerType.get()),
        Types.NestedField.optional(4, "f4", Types.LongType.get()),
        Types.NestedField.optional(5, "f5", Types.FloatType.get()),
        Types.NestedField.optional(6, "f6", Types.DoubleType.get()),
        Types.NestedField.optional(7, "f7", Types.DateType.get()),
        Types.NestedField.optional(8, "f8", Types.TimeType.get()),
        Types.NestedField.optional(9, "f9", Types.TimestampType.withZone()),
        Types.NestedField.optional(10, "f10", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(11, "f11", Types.StringType.get()),
        Types.NestedField.optional(12, "f12", Types.FixedType.ofLength(10)),
        Types.NestedField.optional(13, "f13", Types.BinaryType.get()),
        Types.NestedField.optional(14, "f14", Types.DecimalType.of(19, 9)),
        Types.NestedField.optional(15, "f15", Types.ListType.ofOptional(
            100, Types.IntegerType.get())),
        Types.NestedField.optional(16, "f16", Types.MapType.ofOptional(
            200, 300, Types.StringType.get(), Types.IntegerType.get())),
        Types.NestedField.optional(17, "f17", Types.StructType.of(
            Types.NestedField.required(400, "f17_a", Types.StringType.get())))
    );

    private static final String CATALOG_NAME = "seatunnel";
    private static final IcebergCatalogType CATALOG_TYPE = GLUE;
    private static final String CATALOG_DIR = "bucket/seatunnel-test/";
    private static final String WAREHOUSE = "s3://" + CATALOG_DIR;
    private static Catalog CATALOG;

    public void start() throws IOException {
        initializeIcebergTable();
        batchInsertData();
    }

    private void initializeIcebergTable() {
        CATALOG = new IcebergCatalogFactory(CATALOG_NAME,
            CATALOG_TYPE,
            WAREHOUSE,
            null)
            .create();
        if (!CATALOG.tableExists(TABLE)) {
            CATALOG.createTable(TABLE, SCHEMA);
        }
        table = CATALOG.loadTable(TABLE);
        format = FileFormat.valueOf("PARQUET");
        fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
    }

    private void batchInsertData() throws IOException {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("f1", Long.valueOf(0));
        record.setField("f2", true);
        record.setField("f3", Integer.MAX_VALUE);
        record.setField("f4", Long.MAX_VALUE);
        record.setField("f5", Float.MAX_VALUE);
        record.setField("f6", Double.MAX_VALUE);
        record.setField("f7", LocalDate.now());
        record.setField("f8", LocalTime.now());
        record.setField("f9", OffsetDateTime.now());
        record.setField("f10", LocalDateTime.now());
        record.setField("f11", "test");
        record.setField("f12", "abcdefghij".getBytes());
        record.setField("f13", ByteBuffer.wrap("test".getBytes()));
        record.setField("f14", new BigDecimal("1000000000.000000001"));
        record.setField("f15", Arrays.asList(Integer.MAX_VALUE));
        record.setField("f16", Collections.singletonMap("key", Integer.MAX_VALUE));
        Record structRecord = GenericRecord.create(SCHEMA.findField("f17").type().asStructType());
        structRecord.setField("f17_a", "test");
        record.setField("f17", structRecord);

        List<Record> pendingRows = new ArrayList<>();
        FileAppenderFactory<Record> appenderFactory = createAppenderFactory(null, null, null);

        for (int i = 0; i < 100; i++) {
            pendingRows.add(record.copy("f1", Long.valueOf(i)));
            if (i % 10 == 0) {
                DataFile dataFile = prepareDataFile(pendingRows, appenderFactory);
                table.newRowDelta()
                    .addRows(dataFile)
                    .commit();
                pendingRows.clear();
            }
        }
        if (pendingRows.size()>0){
            DataFile dataFile = prepareDataFile(pendingRows, appenderFactory);
            table.newRowDelta()
                .addRows(dataFile)
                .commit();
            pendingRows.clear();
        }
    }

    protected FileAppenderFactory<Record> createAppenderFactory(List<Integer> equalityFieldIds,
                                                                Schema eqDeleteSchema,
                                                                Schema posDeleteRowSchema) {
        return new GenericAppenderFactory(table.schema(), table.spec(), ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteSchema, posDeleteRowSchema);
    }

    private DataFile prepareDataFile(List<Record> rowSet, FileAppenderFactory<Record> appenderFactory) throws IOException {
        DataWriter<Record> writer = appenderFactory.newDataWriter(createEncryptedOutputFile(), format, partition);
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
}
