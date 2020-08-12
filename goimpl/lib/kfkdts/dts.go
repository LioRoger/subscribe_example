package kfkdts

import (
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

var _ Consumer = &Dts{}

// Dts 阿里云DTS客户端
type Dts struct {
	config   *Config
	closer   chan struct{}
	MsgChan  chan *Message
	ErrChan  chan error
	Consumer *cluster.Consumer
}

// NewDts ...
func NewDts(config *Config) (*Dts, error) {
	clusterConfig := ClusterConfig(config)
	consumer, err := cluster.NewConsumer(config.Hosts, config.GroupID, []string{config.Topic}, clusterConfig)
	if err != nil {
		return nil, err
	}
	dts := &Dts{
		config:   config,
		Consumer: consumer,
		closer:   make(chan struct{}),
		MsgChan:  make(chan *Message),
		ErrChan:  make(chan error),
	}
	go DtsError(dts)
	go DtsNotify(dts)
	go DtsHandler(dts)
	return dts, nil
}

// ClusterConfig ...
func ClusterConfig(config *Config) *cluster.Config {
	cc := cluster.NewConfig()
	cc.Consumer.Return.Errors = true
	cc.Group.Return.Notifications = true
	cc.Net.MaxOpenRequests = 100
	cc.Consumer.Offsets.CommitInterval = 1 * time.Second
	cc.Consumer.Offsets.Initial = config.Offset
	cc.Net.SASL.Enable = true
	cc.Net.SASL.User = config.User + "-" + config.GroupID
	cc.Net.SASL.Password = config.Passwd
	cc.Version = sarama.V0_11_0_0
	return cc
}

// DtsError ...
func DtsError(dts *Dts) {
	for {
		select {
		case err := <-dts.Consumer.Errors():
			dts.ErrChan <- err
		case <-dts.closer:
			return
		}
	}
}

// DtsNotify ...
func DtsNotify(dts *Dts) {
	for {
		select {
		case ntf := <-dts.Consumer.Notifications():
		case <-dts.closer:
			return
		}
	}
}

// DtsHandler ...
func DtsHandler(dts *Dts) {
	for {
		select {
		case msg, ok := <-dts.Consumer.Messages():
			if ok {
				dts.DispatchMessage(msg)
			}
		case <-dts.closer:
			return
		}
	}
}

// DispatchMessage ...
func (d *Dts) DispatchMessage(msg *sarama.ConsumerMessage) {
	newMsg := &Message{
		Timestamp: msg.Timestamp,
		Offset:    msg.Offset,
	}
	err := DtsDecode(msg.Value, newMsg)
	if err != nil {
		return
	}
	d.MsgChan <- newMsg
}

// Consume ...
func (d *Dts) Consume() <-chan *Message {
	return d.MsgChan
}

// Error ...
func (d *Dts) Error() <-chan error {
	return d.ErrChan
}

func (d *Dts) Close() error {
	close(d.closer)
	close(d.MsgChan)
	close(d.ErrChan)
	d.Consumer.Close()
	return nil
}
