package boot;

import com.alibaba.dts.formats.avro.Field;
import com.alibaba.dts.formats.avro.Record;
import common.FieldEntryHolder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordprocessor.FieldConverter;

import java.util.Iterator;
import java.util.List;

import static common.Util.uncompressionObjectName;

/**
 * this class show how to process record attribute and column
 */
public class MysqlRecordPrinter {
    private static final Logger log = LoggerFactory.getLogger(MysqlRecordPrinter.class);
    private static final FieldConverter FIELD_CONVERTER = FieldConverter.getConverter("mysql", null);


    public static String recordToString(Record record) {
        StringBuilder stringBuilder = new StringBuilder(256);
        switch (record.getOperation()) {
            case DDL: {
                appendRecordGeneralInfo(record, stringBuilder);
                String ddl = (String)record.getAfterImages();
                stringBuilder.append("DDL [").append(ddl).append("]");
                break;
            }
            default: {
                List<Field> fields = (List<Field>) record.getFields();
                FieldEntryHolder[] fieldArray = getFieldEntryHolder(record);
                appendRecordGeneralInfo(record, stringBuilder);
                appendFields(fields, fieldArray[0], fieldArray[1], stringBuilder);
                break;
            }
        }

        return stringBuilder.toString();
    }


    private static FieldEntryHolder[] getFieldEntryHolder(Record record) {
        // this is a simple impl, may exist unhandled situation
        FieldEntryHolder[] fieldArray = new FieldEntryHolder[2];

            fieldArray[0] = new FieldEntryHolder((List<Object>) record.getBeforeImages());
            fieldArray[1] = new FieldEntryHolder((List<Object>) record.getAfterImages());

        return fieldArray;
    }

    private static void appendFields(List<Field> fields, FieldEntryHolder before, FieldEntryHolder after, StringBuilder stringBuilder) {
        if (null != fields) {
            Iterator<Field> fieldIterator = fields.iterator();
            while (fieldIterator.hasNext() && before.hasNext() && after.hasNext()) {
                Field field = fieldIterator.next();
                Object toPrintBefore = before.take();
                Object toPrintAfter = after.take();
                appendField(field, toPrintBefore, toPrintAfter, stringBuilder);
            }
        }
    }



    private static void appendField(Field field, Object beforeImage, Object afterImage, StringBuilder stringBuilder) {
        stringBuilder.append("Field [").append(field.getName()).append("]");
        if (null != beforeImage) {
            stringBuilder.append("Before [").append(FIELD_CONVERTER.convert(field, beforeImage).toString()).append("]");
        }
        if (null != afterImage) {
            stringBuilder.append("After [").append(FIELD_CONVERTER.convert(field, afterImage).toString()).append("]");
        }
        stringBuilder.append("\n");
    }

    private static void appendRecordGeneralInfo(Record record, StringBuilder stringBuilder) {
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
        stringBuilder.
                // record id can not be used as unique identifier
                append("recordID [").append(record.getId()).append("]")
                // source info contains which source this record came from
                .append("source [").append(record.getSource()).append("]")
                // db and table name
                .append("dbTable [").append(dbName).append(".").append(tableName).append("]")
                // record type
                .append("recordType [").append(record.getOperation()).append("]")
                // record generate timestamp in source log
                .append("recordTimestamp [").append(record.getSourceTimestamp()).append("]")
                // record extra tag
                .append("extra tags [").append(StringUtils.join(record.getTags(), ",")).append("]");
        stringBuilder.append("\n");

    }

}
