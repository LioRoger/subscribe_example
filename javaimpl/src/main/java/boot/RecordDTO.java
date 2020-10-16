package boot;

import com.alibaba.dts.formats.avro.Operation;
import com.alibaba.dts.formats.avro.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 封装record数据结构
 */
public class RecordDTO {
    private String dbName;
    private String tableName;
    private Long recordId;
    private com.alibaba.dts.formats.avro.Operation recordType;
    private java.lang.Long recordTimestamp;
    private java.util.Map<java.lang.String, java.lang.String> tags;
    private List<FieldDTO> fieldDTOList;

    public RecordDTO(String dbName, String tableName, Long recordId, Operation recordType, Long recordTimestamp, Map<String, String> tags, List<FieldDTO> fieldDTOList) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.recordId = recordId;
        this.recordType = recordType;
        this.recordTimestamp = recordTimestamp;
        this.tags = tags;
        this.fieldDTOList = fieldDTOList;
    }

    public RecordDTO() {

    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getRecordId() {
        return recordId;
    }

    public void setRecordId(Long recordId) {
        this.recordId = recordId;
    }

    public Operation getRecordType() {
        return recordType;
    }

    public void setRecordType(Operation recordType) {
        this.recordType = recordType;
    }

    public Long getRecordTimestamp() {
        return recordTimestamp;
    }

    public void setRecordTimestamp(Long recordTimestamp) {
        this.recordTimestamp = recordTimestamp;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public List<FieldDTO> getFieldDTOList() {
        return fieldDTOList;
    }

    public void setFieldDTOList(List<FieldDTO> fieldDTOList) {
        this.fieldDTOList = fieldDTOList;
    }

    public void appendFieldDTO(FieldDTO field) {
        if (this.fieldDTOList == null) {
            this.fieldDTOList = new ArrayList<>();
        }
        this.fieldDTOList.add(field);
    }

}
