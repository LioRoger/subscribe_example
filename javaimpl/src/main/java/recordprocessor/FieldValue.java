package recordprocessor;

import org.apache.commons.lang3.StringUtils;
import recordprocessor.mysql.JDKEncodingMapper;

import java.io.UnsupportedEncodingException;

public class FieldValue  {
    private String encoding;
    private byte[] bytes;
    public String getEncoding() {
        return encoding;
    }
    public byte[] getValue() {
        return bytes;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    public void setValue(byte[] bytes) {
        this.bytes = bytes;
    }
    @Override
    public String toString() {
        if (null == getValue()) {
            return "null [binary]";
        }
        if (encoding==null) {
            return super.toString();
        }
        try {
            if(StringUtils.equals("utf8mb4", encoding)){
                return new String(getValue(), "utf8");
            }else{
                return new String(getValue(), encoding);
            }
        } catch (UnsupportedEncodingException e) {
            String  realEncoding = JDKEncodingMapper.getJDKEncoding(encoding);
            if (null == realEncoding) {
                throw new RuntimeException("Unsupported encoding: " + encoding);
            } else {
                try {
                    return new String(getValue(), realEncoding);
                } catch (UnsupportedEncodingException e1) {
                    throw new RuntimeException("Unsupported encoding: origin " +  encoding + ", mapped " + realEncoding);
                }
            }
        }
    }
}
