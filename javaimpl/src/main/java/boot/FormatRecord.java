package boot;

import com.alibaba.dts.formats.avro.Field;
import com.alibaba.dts.formats.avro.Record;
import common.FieldEntryHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordprocessor.FieldConverter;

import java.util.Iterator;
import java.util.List;

import static common.Util.uncompressionObjectName;

/**
 * 格式化成 {@link boot.RecordDTO} 数据对象。
 */
public class FormatRecord {
    private static final Logger log = LoggerFactory.getLogger(MysqlRecordPrinter.class);
    private static final FieldConverter FIELD_CONVERTER = FieldConverter.getConverter("mysql", null);


    public static RecordDTO format(Record record) {
        RecordDTO recordDTO = new RecordDTO();
        switch (record.getOperation()) {
            case DDL: {
                appendRecordGeneralInfo(record, recordDTO);
                break;
            }
            default: {
                List<Field> fields = (List<Field>) record.getFields();
                FieldEntryHolder[] fieldArray = getFieldEntryHolder(record);
                appendRecordGeneralInfo(record, recordDTO);
                appendFields(fields, fieldArray[0], fieldArray[1], recordDTO);
                break;
            }
        }
        return recordDTO;
    }


    private static FieldEntryHolder[] getFieldEntryHolder(Record record) {
        // this is a simple impl, may exist unhandled situation
        FieldEntryHolder[] fieldArray = new FieldEntryHolder[2];

        fieldArray[0] = new FieldEntryHolder((List<Object>) record.getBeforeImages());
        fieldArray[1] = new FieldEntryHolder((List<Object>) record.getAfterImages());

        return fieldArray;
    }

    private static void appendFields(List<Field> fields, FieldEntryHolder before, FieldEntryHolder after, RecordDTO recordDTO) {
        if (null != fields) {
            Iterator<Field> fieldIterator = fields.iterator();
            while (fieldIterator.hasNext() && before.hasNext() && after.hasNext()) {
                Field field = fieldIterator.next();
                Object toPrintBefore = before.take();
                Object toPrintAfter = after.take();
                appendField(field, toPrintBefore, toPrintAfter, recordDTO);
            }
        }
    }

    private static void appendField(Field field, Object beforeImage, Object afterImage, RecordDTO recordDTO) {
        FieldDTO fieldDTO = new FieldDTO();
        fieldDTO.setFieldName(field.getName());
        if (null != beforeImage) {
            fieldDTO.setBefore(FIELD_CONVERTER.convert(field, beforeImage).toString());
        }
        if (null != afterImage) {
            fieldDTO.setAfter(FIELD_CONVERTER.convert(field, afterImage).toString());
        }
        recordDTO.appendFieldDTO(fieldDTO);
    }

    private static void appendRecordGeneralInfo(Record record, RecordDTO recordDTO) {
        String dbName = null;
        String tableName = null;
        // here we get db and table name
        String[] dbPair = uncompressionObjectName(record.getObjectName());
        if (null != dbPair) {
            if (dbPair.length == 2) {
                dbName = dbPair[0];
                tableName = dbPair[1];
            } else if (dbPair.length == 3) {
                dbName = dbPair[0];
                tableName = dbPair[2];
            } else if (dbPair.length == 1) {
                dbName = dbPair[0];
                tableName = "";
            } else {
                throw new RuntimeException("invalid db and table name pair for record [" + record + "]");
            }
        }
        recordDTO.setRecordId(record.getId());
        recordDTO.setDbName(dbName);
        recordDTO.setTableName(tableName);
        recordDTO.setRecordType(record.getOperation());
        recordDTO.setRecordTimestamp(record.getSourceTimestamp());
        recordDTO.setTags(record.getTags());
    }

}
