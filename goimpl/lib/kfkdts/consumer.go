package kfkdts

import (
	"time"

	"github.com/LioRoger/subscribe_example/goimpl/lib/dtsavro"
)

// Consumer 消费者接口
type Consumer interface {
	Consume() <-chan *Message
	Close() error
	Error() <-chan error
}

// NewConsumer ...
func NewConsumer(config *Config) (Consumer, error) {
	if config.IsAliyunDTS {
		return NewDts(config)
	}
	return NewKfk(config)
}

// Message 消息定义
// 阿里云DTS或Kafka过来的消息统一解析成Message
type Message struct {
	Type      dtsavro.Operation
	Offset    int64
	Timestamp time.Time
	Event     *Event
}

// Event 事件消息结构
// 示例：
//{
//    "data": [
//        {
//            "id": "47",
//            "created_at": "2020-07-14 13:09:19",
//            "updated_at": null,
//            "deleted_at": null,
//            "type_id": "2",
//            "subscribed": "1",
//            "test_user_id": "2"
//        }
//    ],
//    "database": "test",
//    "es": 1596102215000,
//    "id": 3,
//    "isDdl": false,
//    "mysqlType": {
//        "id": "int(10) unsigned",
//        "created_at": "datetime",
//        "updated_at": "datetime",
//        "deleted_at": "datetime",
//        "type_id": "int(11)",
//        "subscribed": "tinyint(1)",
//        "test_user_id": "int(11)"
//    },
//    "old": [
//        {
//            "subscribed": null
//        }
//    ],
//    "pkNames": [
//        "id"
//    ],
//    "sql": "",
//    "sqlType": {
//        "id": 4,
//        "created_at": 93,
//        "updated_at": 93,
//        "deleted_at": 93,
//        "type_id": 4,
//        "subscribed": -7,
//        "test_user_id": 4
//    },
//    "table": "test_emails",
//    "ts": 1596102215214,
//    "type": "UPDATE"
//}
type Event struct {
	Type     string              `json:"type"`
	Database string              `json:"database"`
	Table    string              `json:"table"`
	Data     []map[string]string `json:"data"`
	Old      []map[string]string `json:"old"`
	//Es        int64               `json:"es"`
	//ID        int                 `json:"id"`
	//IsDdl     bool                `json:"isDdl"`
	//MysqlType map[string]string   `json:"mysqlType"`
	//PkNames   []string            `json:"pkNames"`
	//SQL       string              `json:"sql"`
	//SQLType   map[string]int32    `json:"sqlType"`
	//Ts        int64               `json:"ts"`
}
