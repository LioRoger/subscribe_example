package common;

import com.alibaba.dts.formats.avro.Record;

public  interface RecordListener {


    public void consume(UserRecord record);

}
