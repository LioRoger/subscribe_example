package main

import (
	"bytes"
	//this is package you build in local space
	"dtsavro"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/actgardner/gogen-avro/v7/compiler"
	"github.com/actgardner/gogen-avro/v7/vm"
	cluster "github.com/bsm/sarama-cluster"
	"io"
	"os"
	"os/signal"
	"time"
)

var ()

func main() {

	var (
		r io.Reader
	)
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Net.MaxOpenRequests = 100
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	//config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "user-groupid"
	config.Net.SASL.Password = "password"
	config.Version = sarama.V0_11_0_0

	consumer, err := cluster.NewConsumer([]string{"brokerURL"}, "groupid", []string{"topicName"}, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			panic(err)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Println("Rebalanced: %+v\n", ntf)
		}
	}()

	// Pre compile schema of avro
	t := dtsavro.NewRecord()
	deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
	if err != nil {
		panic(err)
	}
	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key)
				r = bytes.NewReader(msg.Value)
				t = dtsavro.NewRecord()
				if err = vm.Eval(r, deser, t); err != nil {
					panic(err)
				}
				fmt.Println(t.Operation, t.ObjectName.String, t.Tags)
				for _, j := range t.Fields.ArrayField {
					fmt.Println(j.Name, j.DataTypeNumber)
				}
			}
		case <-signals:
			return
		}
	}
}
