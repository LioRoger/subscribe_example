package kfkdts

import (
	"bytes"
	"errors"
	"strings"

	"github.com/LioRoger/subscribe_example/goimpl/lib/dtsavro"
	"github.com/linkedin/goavro"
	"github.com/mitchellh/mapstructure"
)

func DtsDecode(value []byte, message *Message) error {
	if message.Event == nil {
		message.Event = &Event{
			Data: make([]map[string]string, 0),
		}
	}
	r := bytes.NewReader(value)
	t, err := dtsavro.DeserializeRecord(r)
	if err != nil {
		return err
	}
	message.Type = t.Operation
	if message.Type != dtsavro.OperationINSERT &&
		message.Type != dtsavro.OperationUPDATE &&
		message.Type != dtsavro.OperationDELETE {
		return errors.New("type [" + t.Operation.String() + "] not supported")
	}
	if err = getTableName(t.ObjectName.String, message); err != nil {
		return err
	}
	codec, err := goavro.NewCodec(t.Schema())
	if err != nil {
		return err
	}
	native, _, err := codec.NativeFromBinary(value)
	if err != nil {
		return err
	}
	record := &Record{}
	if err := mapstructure.Decode(native, record); err != nil {
		return err
	}
	afterData := make([]map[string]string, 0)
	beforeData := make([]map[string]string, 0)
	afterColomns := make(map[string]string)
	beforeColomns := make(map[string]string)
	// 解析表的每列
	for i, v := range record.Fields.Array {
		if record.BeforeImages != nil {
			value := GetSqlValue(v.DataTypeNumber, record.BeforeImages.Array[i])
			beforeColomns[v.Name] = value
		}
		if record.AfterImages != nil {
			value := GetSqlValue(v.DataTypeNumber, record.AfterImages.Array[i])
			afterColomns[v.Name] = value
		}
	}
	afterData = append(afterData, afterColomns)
	message.Event.Data = afterData
	beforeData = append(beforeData, beforeColomns)
	message.Event.Old = beforeData
	return nil
}

func getTableName(objectName string, message *Message) error {
	table := strings.Split(objectName, ".")
	if len(table) == 2 {
		message.Event.Database = table[0]
		message.Event.Table = table[1]
		return nil
	} else if len(table) == 3 {
		message.Event.Database = table[0]
		message.Event.Table = table[2]
		return nil
	} else if len(table) == 1 {
		message.Event.Database = table[0]
		message.Event.Table = ""
		return nil
	}
	return errors.New("get table name failed")
}

type Record struct {
	Version            int32             `json:"version"`
	Id                 int64             `json:"id"`
	SourceTimestamp    int64             `json:"sourceTimestamp"`
	SourcePosition     string            `json:"sourcePosition"`
	SafeSourcePosition string            `json:"safeSourcePosition"`
	SourceTxid         string            `json:"sourceTxid"`
	Source             map[string]string `json:"source"`
	Operation          string            `json:"operation"`
	ObjectName         map[string]string `json:"objectName"`
	ProcessTimestamps  int64             `json:"processTimestamps"`
	Tags               map[string]string `json:"tags"`
	Fields             *Fields           `json:"fields"`
	BeforeImages       *Images           `json:"beforeImages"`
	AfterImages        *Images           `json:"afterImages"`
	BornTimestamp      int64             `json:"bornTimestamp"`
}

type FArray struct {
	DataTypeNumber int    `json:"dataTypeNumber"`
	Name           string `json:"name"`
}
type Fields struct {
	Array []FArray `json:"array"`
}

type Images struct {
	Array []map[string]map[string]interface{} `json:"array"`
}
