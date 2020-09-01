package kfkdts

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKfk(t *testing.T) {
	config := &Config{
		IsAliyunDTS: false,
		User:        "",
		Passwd:      "",
		Hosts:       []string{"127.0.0.1:9092"},
		Topic:       "topic",
		GroupID:     "",
		Partition:   0,
		Offset:      -2, // -1:OffsetNewest -2:OffsetOldest
	}
	consumer, err := NewConsumer(config)
	if err != nil {
		assert.Equal(t, err, nil)
	}
	for {
		select {
		case msg := <-consumer.Consume():
			fmt.Printf("recv msg type[%s] timestamp[%s] offset[%d] database[%s] table[[%s]",
				msg.Type.String(), msg.Timestamp.String(), msg.Offset, msg.Event.Database, msg.Event.Table)
			if msg.Event.Database == "test" && msg.Event.Table == "test_emails" {
				for _, data := range msg.Event.Data {
					for k, v := range data {
						fmt.Printf("colomn[%s,%s]", k, v)
					}
				}
			}
		case err := <-consumer.Error():
			break
		}
	}
	consumer.Close()
}

func TestDts(t *testing.T) {
	config := &Config{
		IsAliyunDTS: true,
		User:        "username",
		Passwd:      "passwd",
		Hosts:       []string{"brokerUrl:port"},
		Topic:       "topicName",
		GroupID:     "groupid",
		Partition:   0,
		Offset:      -2, // -1:OffsetNewest -2:OffsetOldest
	}
	consumer, err := NewConsumer(config)
	if err != nil {
		assert.Equal(t, err, nil)
	}
	for {
		select {
		case msg := <-consumer.Consume():
			fmt.Printf("recv msg type[%s] timestamp[%s] offset[%d] database[%s] table[[%s]",
				msg.Type.String(), msg.Timestamp.String(), msg.Offset, msg.Event.Database, msg.Event.Table)
			for _, data := range msg.Event.Data {
				for k, v := range data {
					fmt.Printf("colomn[%s,%s]", k, v)
				}
			}
		case err := <-consumer.Error():
			break
		}
	}
	consumer.Close()
}
