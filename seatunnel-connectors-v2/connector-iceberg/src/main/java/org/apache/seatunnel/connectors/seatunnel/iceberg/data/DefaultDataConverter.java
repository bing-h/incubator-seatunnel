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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import static org.apache.iceberg.types.Type.TypeID.BOOLEAN;
import static org.apache.iceberg.types.Type.TypeID.DOUBLE;
import static org.apache.iceberg.types.Type.TypeID.FIXED;
import static org.apache.iceberg.types.Type.TypeID.FLOAT;
import static org.apache.iceberg.types.Type.TypeID.INTEGER;
import static org.apache.iceberg.types.Type.TypeID.LONG;
import static org.apache.iceberg.types.Type.TypeID.STRING;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Slf4j
public class DefaultDataConverter implements DataConverter {

    @NonNull
    private final SeaTunnelRowType seaTunnelRowType;
    @NonNull
    private final Schema icebergSchema;

    private Map<Type.TypeID, Object[]> arrayTypeMap = new HashMap<Type.TypeID, Object[]>() {
        {
            put(BOOLEAN, new Boolean[0]);
            put(INTEGER, new Integer[0]);
            put(LONG, new Long[0]);
            put(FLOAT, new Float[0]);
            put(DOUBLE, new Double[0]);
            put(STRING, new String[0]);
        }
    };

    @Override
    public SeaTunnelRow toSeaTunnelRowStruct(@NonNull Record record) {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
            Types.NestedField icebergField = icebergSchema.findField(seaTunnelFieldName);
            Object icebergValue = record.getField(seaTunnelFieldName);
            seaTunnelRow.setField(i, convertToSeaTunnel(icebergField.type(), icebergValue, seaTunnelFieldType));
        }
        return seaTunnelRow;
    }

    @Override
    public Record toIcebergStruct(SeaTunnelRow row) {
        GenericRecord genericRecord = GenericRecord.create(icebergSchema);
        for (int i = 0; i < row.getArity(); i++) {
            String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
            Types.NestedField icebergField = icebergSchema.findField(seaTunnelFieldName);
            Object value = row.getField(i);
            genericRecord.setField(seaTunnelFieldName, convertToIceberg(seaTunnelFieldType, icebergField.type(), value));
        }
        return genericRecord;
    }

    private Object convertToIceberg(@NonNull SeaTunnelDataType<?> seaTunnelType, @NonNull Type icebergType, Object seaTunnelValue) {
        if (seaTunnelValue == null) {
            return null;
        }
        switch (seaTunnelType.getSqlType()) {
            case STRING:
                return String.class.cast(seaTunnelValue);
            case BOOLEAN:
                return Boolean.class.cast(seaTunnelValue);
            case INT:
                return Integer.class.cast(seaTunnelValue);
            case BIGINT:
                return Long.class.cast(seaTunnelValue);
            case FLOAT:
                return Float.class.cast(seaTunnelValue);
            case DOUBLE:
                return Double.class.cast(seaTunnelValue);
            case DECIMAL:
                return BigDecimal.class.cast(seaTunnelValue);
            case BYTES:
                if (icebergType.typeId() == FIXED) {
                    return byte[].class.cast(seaTunnelValue);
                }
                return ByteBuffer.wrap(byte[].class.cast(seaTunnelValue));
            case DATE:
                return LocalDate.class.cast(seaTunnelValue);
            case TIME:
                return LocalTime.class.cast(seaTunnelValue);
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    ZoneOffset utc = ZoneOffset.UTC;
                    return ((LocalDateTime) seaTunnelValue).atOffset(utc);
                }
                return LocalDateTime.class.cast(seaTunnelValue);
            case ROW:
                SeaTunnelRow seaTunnelRow = SeaTunnelRow.class.cast(seaTunnelValue);
                Types.StructType icebergStructType = (Types.StructType) icebergType;
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelType;
                GenericRecord record = GenericRecord.create(icebergStructType);
                for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                    String fieldName = seaTunnelRowType.getFieldName(i);
                    Object fieldValue = convertToIceberg(seaTunnelRowType.getFieldType(i), icebergStructType.fieldType(fieldName), seaTunnelRow.getField(i));
                    record.setField(fieldName, fieldValue);
                }
                return record;
            case ARRAY:
                Object[] seaTunnelList = Object[].class.cast(seaTunnelValue);
                Types.ListType icebergListType = (Types.ListType) icebergType;
                List icebergList = new ArrayList(seaTunnelList.length);
                ArrayType seatunnelListType = (ArrayType) seaTunnelType;
                for (int i = 0; i < seaTunnelList.length; i++) {
                    icebergList.add(convertToIceberg(seatunnelListType.getElementType(), icebergListType.elementType(), seaTunnelList[i]));
                }
                return icebergList;
            case MAP:
                Map<Object, Object> seaTunnelMap = Map.class.cast(seaTunnelValue);
                Types.MapType icebergMapType = (Types.MapType) icebergType;
                Map icebergMap = new HashMap();
                MapType seaTunnelMapType = (MapType) seaTunnelType;
                for (Map.Entry entry : seaTunnelMap.entrySet()) {
                    icebergMap.put(convertToIceberg(seaTunnelMapType.getKeyType(), icebergMapType.keyType(), entry.getKey()), convertToIceberg(seaTunnelMapType.getValueType(), icebergMapType.valueType(), entry.getValue()));
                }
                return icebergMap;
            default:
                throw new UnsupportedOperationException("Unsupported seatunnel type: " + seaTunnelType);
        }
    }

    private Object convertToSeaTunnel(@NonNull Type icebergType, Object icebergValue, @NonNull SeaTunnelDataType<?> seaTunnelType) {
        if (icebergValue == null) {
            return null;
        }
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return Boolean.class.cast(icebergValue);
            case INTEGER:
                return Integer.class.cast(icebergValue);
            case LONG:
                return Long.class.cast(icebergValue);
            case FLOAT:
                return Float.class.cast(icebergValue);
            case DOUBLE:
                return Double.class.cast(icebergValue);
            case DATE:
                return LocalDate.class.cast(icebergValue);
            case TIME:
                return LocalTime.class.cast(icebergValue);
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    return OffsetDateTime.class.cast(icebergValue).toLocalDateTime();
                }
                return LocalDateTime.class.cast(icebergValue);
            case STRING:
                return String.class.cast(icebergValue);
            case FIXED:
                return byte[].class.cast(icebergValue);
            case BINARY:
                return ByteBuffer.class.cast(icebergValue).array();
            case DECIMAL:
                return BigDecimal.class.cast(icebergValue);
            case STRUCT:
                Record icebergStruct = Record.class.cast(icebergValue);
                Types.StructType icebergStructType = (Types.StructType) icebergType;
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelType;
                SeaTunnelRow seatunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
                for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                    String seatunnelFieldName = seaTunnelRowType.getFieldName(i);
                    Object seatunnelFieldValue = convertToSeaTunnel(icebergStructType.fieldType(seatunnelFieldName), icebergStruct.getField(seatunnelFieldName), seaTunnelRowType.getFieldType(i));
                    seatunnelRow.setField(i, seatunnelFieldValue);
                }
                return seatunnelRow;
            case LIST:
                List icebergList = List.class.cast(icebergValue);
                List seatunnelList = new ArrayList();
                Type.TypeID typeID = ((Types.ListType) icebergType).elementType().typeId();
                Types.ListType icebergListType = (Types.ListType) icebergType;
                ArrayType seatunnelListType = (ArrayType) seaTunnelType;
                for (int i = 0; i < icebergList.size(); i++) {
                    seatunnelList.add(convertToSeaTunnel(icebergListType.elementType(), icebergList.get(i), seatunnelListType.getElementType()));
                }
                return seatunnelList.toArray(arrayTypeMap.get(typeID));
            case MAP:
                Map<Object, Object> icebergMap = Map.class.cast(icebergValue);
                Types.MapType icebergMapType = (Types.MapType) icebergType;
                Map seatunnelMap = new HashMap();
                MapType seatunnelMapType = (MapType) seaTunnelType;
                for (Map.Entry entry : icebergMap.entrySet()) {
                    seatunnelMap.put(convertToSeaTunnel(icebergMapType.keyType(), entry.getKey(), seatunnelMapType.getKeyType()), convertToSeaTunnel(icebergMapType.valueType(), entry.getValue(), seatunnelMapType.getValueType()));
                }
                return seatunnelMap;
            default:
                throw new IcebergConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format("Unsupported iceberg type: %s", icebergType));
        }
    }
}
