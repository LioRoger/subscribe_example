package kfkdts

import (
	"fmt"
)

// 定义参考: https://www.alibabacloud.com/help/zh/doc-detail/121239.html
const (
	MysqlTypeDecimal      = 0
	MysqlTypeInt8         = 1
	MysqlTypeInt16        = 2
	MysqlTypeInt32        = 3
	MysqlTypeFloat        = 4
	MysqlTypeDouble       = 5
	MysqlTypeNull         = 6
	MysqlTypeTimestamp    = 7
	MysqlTypeInt64        = 8
	MysqlTypeInt24        = 9
	MysqlTypeDate         = 10
	MysqlTypeTime         = 11
	MysqlTypeDatetime     = 12
	MysqlTypeYear         = 13
	MysqlTypeDateNew      = 14
	MysqlTypeVarchar      = 15
	MysqlTypeBit          = 16
	MysqlTypeTimestampNew = 17
	MysqlTypeDatetimeNew  = 18
	MysqlTypeTimeNew      = 19
	MysqlTypeJSON         = 245
	MysqlTypeDecimalNew   = 246
	MysqlTypeEnum         = 247
	MysqlTypeSet          = 248
	MysqlTypeTinyBlob     = 249
	MysqlTypeMediumBlob   = 250
	MysqlTypeLongBlob     = 251
	MysqlTypeBlob         = 252
	MysqlTypeVarString    = 253
	MysqlTypeString       = 254
	MysqlTypeGeometry     = 255
)

// GetSqlValue ...
// 目前没有全部解析，需要拿到全部样式后完成 2020/08/12
func GetSqlValue(mysqlType int, data map[string]map[string]interface{}) string {
	for _, value := range data {
		switch mysqlType {
		case MysqlTypeDecimal:
			// dtsavro.Decimal
			return value["value"].(string)
		case MysqlTypeInt8:
			fallthrough
		case MysqlTypeInt16:
			fallthrough
		case MysqlTypeInt24:
			fallthrough
		case MysqlTypeInt32:
			fallthrough
		case MysqlTypeInt64:
			//dtsavro.Integer
			return value["value"].(string)
		case MysqlTypeFloat:
			fallthrough
		case MysqlTypeDouble:
			// dtsavro.Float
			return fmt.Sprintf("%f", value["value"].(float64))
		case MysqlTypeNull:
			return ""
		case MysqlTypeTimestamp:
			// dtsavro.Timestamp
			return fmt.Sprintf("%d", value["timestamp"].(int64))
		case MysqlTypeDate:
		case MysqlTypeTime:
		case MysqlTypeDatetime:
		case MysqlTypeYear:
		case MysqlTypeDateNew:
		case MysqlTypeVarchar:
		case MysqlTypeBit:
		case MysqlTypeTimestampNew:
		case MysqlTypeDatetimeNew:
		case MysqlTypeTimeNew:
		case MysqlTypeJSON:
			// dtsavro.TextObject
			return value["value"].(string)
		case MysqlTypeDecimalNew:
			// dtsavro.Decimal
			return value["value"].(string)
		case MysqlTypeEnum:
		case MysqlTypeSet:
		case MysqlTypeTinyBlob:
			fallthrough
		case MysqlTypeMediumBlob:
			fallthrough
		case MysqlTypeLongBlob:
			fallthrough
		case MysqlTypeBlob:
			fallthrough
		case MysqlTypeVarString:
			// dtsavro.Character
			return B2S(value["value"].([]uint8))
		case MysqlTypeString:
			// dtsavro.Character
			return B2S(value["value"].([]uint8))
		case MysqlTypeGeometry:
			// dtsavro.TextEgometry
			return value["value"].(string)
		default:
		}
	}
	return ""
}

func B2S(bs []uint8) string {
	ba := []byte{}
	for _, b := range bs {
		ba = append(ba, byte(b))
	}
	return string(ba)
}
