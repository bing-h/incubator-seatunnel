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

import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.GLUE;

public class IcebergSinkIT {
    public static void main(String[] args) {
        new IcebergSinkIT().start();
    }

    private static final TableIdentifier TABLE =
            TableIdentifier.of(Namespace.of("database1"), "sink");
    private static final Schema SCHEMA =
            new Schema(
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
                    Types.NestedField.optional(
                            15, "f15", Types.ListType.ofOptional(100, Types.IntegerType.get())),
                    Types.NestedField.optional(
                            16,
                            "f16",
                            Types.MapType.ofOptional(
                                    200, 300, Types.StringType.get(), Types.IntegerType.get())),
                    Types.NestedField.optional(
                            17,
                            "f17",
                            Types.StructType.of(
                                    Types.NestedField.required(
                                            400, "f17_a", Types.StringType.get()))));

    private static final String CATALOG_NAME = "seatunnel";
    private static final IcebergCatalogType CATALOG_TYPE = GLUE;
    private static final String CATALOG_DIR = "bytepower-log/seatunnel-test/";
    private static final String WAREHOUSE = "s3://" + CATALOG_DIR;
    private static Catalog CATALOG;

    public void start() {
        initializeIcebergTable();
    }

    private void initializeIcebergTable() {
        CATALOG = new IcebergCatalogFactory(CATALOG_NAME, CATALOG_TYPE, WAREHOUSE, null).create();
        if (!CATALOG.tableExists(TABLE)) {
            CATALOG.createTable(TABLE, SCHEMA);
        }
    }
}
