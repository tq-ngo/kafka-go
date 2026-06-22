package main

import (
	"os"
	"testing"
)

func BenchmarkQueuePushPop(b *testing.B) {
	q := &Queue{}
	q.init(999, 999, 999)

	defer func() {
		q.deinit()
		os.Remove("partition_metadata_999_999_999.dat")
		os.Remove("underArr_999_999_999.dat")
		os.Remove("underSize_999_999_999.dat")
	}()

	messagePayload := []byte("benchmarking_sample_payload_data_12345")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.push(messagePayload)
		_ = q.pop()
	}
}
