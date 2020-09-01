package kfkdts

import (
	"encoding/json"

	"github.com/LioRoger/subscribe_example/goimpl/lib/dtsavro"
	"github.com/Shopify/sarama"
)

var _ Consumer = &Kfk{}

// Kfk 卡夫卡客户端定义
type Kfk struct {
	config    *Config
	consumer  sarama.Consumer
	Pconsumer sarama.PartitionConsumer
	closer    chan struct{}
	MsgChan   chan *Message
	ErrChan   chan error
}

// NewKfk ...
func NewKfk(config *Config) (*Kfk, error) {
	kconfig := sarama.NewConfig()
	kconfig.Consumer.Return.Errors = true
	kconfig.Version = sarama.V0_11_0_0
	c, err := sarama.NewConsumer(config.Hosts, kconfig)
	if err != nil {
		return nil, err
	}
	pConsumer, err := c.ConsumePartition(config.Topic, config.Partition, config.Offset)
	if err != nil {
		return nil, err
	}
	kfk := &Kfk{
		config:    config,
		consumer:  c,
		Pconsumer: pConsumer,
		closer:    make(chan struct{}),
		MsgChan:   make(chan *Message),
		ErrChan:   make(chan error),
	}
	go KfkHandler(kfk)
	return kfk, nil
}

// KfkHandler ...
func KfkHandler(kfk *Kfk) {
	for {
		select {
		case msg := <-kfk.Pconsumer.Messages():
			kfk.DispatchMessage(msg)
		case err := <-kfk.Pconsumer.Errors():
			kfk.ErrChan <- err
		case <-kfk.closer:
			return
		}
	}
}

// DisaptchMessage 解析kafka过来的消息成Message并放入channel
func (k *Kfk) DispatchMessage(msg *sarama.ConsumerMessage) {
	e := &Event{}
	if err := json.Unmarshal(msg.Value, e); err != nil {
		return
	}
	if e.Type == dtsavro.OperationUPDATE.String() {
		k.MsgChan <- &Message{
			Type:      dtsavro.OperationUPDATE,
			Timestamp: msg.Timestamp,
			Offset:    msg.Offset,
			Event:     e,
		}
	}
	if e.Type == dtsavro.OperationINSERT.String() {
		k.MsgChan <- &Message{
			Type:      dtsavro.OperationINSERT,
			Timestamp: msg.Timestamp,
			Offset:    msg.Offset,
			Event:     e,
		}
	}
	if e.Type == dtsavro.OperationDELETE.String() {
		k.MsgChan <- &Message{
			Type:      dtsavro.OperationDELETE,
			Timestamp: msg.Timestamp,
			Offset:    msg.Offset,
			Event:     e,
		}
	}
}

// Consume ...
func (k *Kfk) Consume() <-chan *Message {
	return k.MsgChan
}

// Error ...
func (k *Kfk) Error() <-chan error {
	return k.ErrChan
}

// Close ...
func (k *Kfk) Close() error {
	close(k.closer)
	close(k.MsgChan)
	close(k.ErrChan)
	k.consumer.Close()
	k.Pconsumer.Close()
	return nil
}
