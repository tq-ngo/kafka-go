package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	globalInit           sync.Once
	globalProducerStream *bufio.ReadWriter
	globalConsumerStream *bufio.ReadWriter
)

func initGlobalBenchmarkEnv() {
	// Clean up data files to guarantee a clean starting environment
	os.Remove("broker_metadata.dat")
	os.Remove("topic_metadata_1.dat")
	os.Remove("cgroup_metadata_1_1.dat")
	os.Remove("partition_metadata_1_1_1.dat")
	os.Remove("underArr_1_1_1.dat")
	os.Remove("underSize_1_1_1.dat")
	os.Remove("partition_metadata_1_65535_65535.dat")
	os.Remove("underArr_1_65535_65535.dat")
	os.Remove("underSize_1_65535_65535.dat")

	// 1. Spin up the Broker Server in a background thread
	broker := Broker{}
	broker.init()
	go func() {
		_ = broker.startBrokerServer()
	}()
	time.Sleep(100 * time.Millisecond) // Await port 10000 binding

	// 2. Open listeners for dialed-back client pipelines
	cLn, err := net.Listen("tcp", ":10002")
	if err != nil {
		panic(err)
	}
	pLn, err := net.Listen("tcp", ":10001")
	if err != nil {
		panic(err)
	}

	// 3. Register Consumer FIRST to initialize active CG partitions
	cConn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		panic(err)
	}
	cRegStream := bufio.NewReadWriter(bufio.NewReader(cConn), bufio.NewWriter(cConn))
	cRegMsg := ConsumerRegisterMessage{port: 10002, topicID: 1, groupID: 1}
	_ = writeMessageToStream(cRegStream, Message{C_REG: &cRegMsg})
	_, _ = readMessageFromStream(cRegStream)
	cConn.Close()

	brokerCConn, err := cLn.Accept()
	if err != nil {
		panic(err)
	}
	globalConsumerStream = bufio.NewReadWriter(bufio.NewReader(brokerCConn), bufio.NewWriter(brokerCConn))

	// 4. Register Producer SECOND
	pConn, err := net.Dial("tcp", fmt.Sprintf(":%d", BROKER_PORT))
	if err != nil {
		panic(err)
	}
	pRegStream := bufio.NewReadWriter(bufio.NewReader(pConn), bufio.NewWriter(pConn))
	pRegMsg := ProducerRegisterMessage{port: 10001, topicID: 1}
	_ = writeMessageToStream(pRegStream, Message{P_REG: &pRegMsg})
	_, _ = readMessageFromStream(pRegStream)
	pConn.Close()

	brokerPConn, err := pLn.Accept()
	if err != nil {
		panic(err)
	}
	globalProducerStream = bufio.NewReadWriter(bufio.NewReader(brokerPConn), bufio.NewWriter(brokerPConn))
}

func BenchmarkEndToEndThroughput(b *testing.B) {
	// Initialize everything exactly once across all testing framework sub-runs
	globalInit.Do(initGlobalBenchmarkEnv)

	payload := []byte("benchmark_high_speed_payload_data_packet")
	var readyAck byte = 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 1. Producer passes data payload packet to the broker
		err := writeMessageToStream(globalProducerStream, Message{PCM: payload})
		if err != nil {
			b.Fatalf("Failed writing on iteration %d: %v", i, err)
		}

		// 2. Producer awaits transaction response from broker
		resp, err := readMessageFromStream(globalProducerStream)
		if err != nil || resp == nil || resp.R_PCM == nil {
			b.Fatalf("Failed reading producer ACK on iteration %d: %v", i, err)
		}

		// 3. Consumer takes the pushed data packet downstream
		msg, err := readMessageFromStream(globalConsumerStream)
		if err != nil || msg == nil || msg.PCM == nil {
			b.Fatalf("Failed consumer message read on iteration %d: %v", i, err)
		}

		// 4. Consumer returns a readiness acknowledgement to the broker
		err = writeMessageToStream(globalConsumerStream, Message{R_PCM: &readyAck})
		if err != nil {
			b.Fatalf("Failed writing consumer status flag on iteration %d: %v", i, err)
		}
	}
}
