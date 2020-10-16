package boot;

/**
 * 封装field相关的数据结构
 */
public class FieldDTO {
    private String fieldName;
    private String before;
    private String after;

    public FieldDTO(String fieldName, String before, String after) {
        this.fieldName = fieldName;
        this.before = before;
        this.after = after;
    }

    public FieldDTO() {

    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getBefore() {
        return before;
    }

    public void setBefore(String before) {
        this.before = before;
    }

    public String getAfter() {
        return after;
    }

    public void setAfter(String after) {
        this.after = after;
    }
}
