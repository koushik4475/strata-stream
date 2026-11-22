package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func writeInt16(buf *bytes.Buffer, value int16) {
	_ = binary.Write(buf, binary.BigEndian, value)
}

func writeInt32(buf *bytes.Buffer, value int32) {
	_ = binary.Write(buf, binary.BigEndian, value)
}

func writeInt64(buf *bytes.Buffer, value int64) {
	_ = binary.Write(buf, binary.BigEndian, value)
}

func malformedProducePayload(topicLen int16, topic string, messageCount int32, tail func(*bytes.Buffer)) *bytes.Reader {
	buf := new(bytes.Buffer)
	writeInt16(buf, topicLen)
	buf.WriteString(topic)
	writeInt32(buf, 0) // partition
	writeInt32(buf, messageCount)
	if tail != nil {
		tail(buf)
	}
	return bytes.NewReader(buf.Bytes())
}

func TestDecodeProduceRequestRejectsInvalidLengths(t *testing.T) {
	tests := []struct {
		name string
		body *bytes.Reader
	}{
		{
			name: "negative topic length",
			body: malformedProducePayload(-1, "", 0, nil),
		},
		{
			name: "topic length exceeds remaining frame",
			body: malformedProducePayload(5, "t", 0, nil),
		},
		{
			name: "negative message count",
			body: malformedProducePayload(1, "t", -1, nil),
		},
		{
			name: "message count exceeds remaining frame",
			body: malformedProducePayload(1, "t", 1, nil),
		},
		{
			name: "key length below null sentinel",
			body: malformedProducePayload(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt32(buf, -2)
			}),
		},
		{
			name: "key length exceeds remaining frame",
			body: malformedProducePayload(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt32(buf, 100)
			}),
		},
		{
			name: "value length below null sentinel",
			body: malformedProducePayload(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt32(buf, 0)
				writeInt32(buf, -2)
			}),
		},
		{
			name: "value length exceeds remaining frame",
			body: malformedProducePayload(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt32(buf, 0)
				writeInt32(buf, 100)
			}),
		},
	}

	codec := &WireCodec{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := codec.DecodeProduceRequest(tt.body); err == nil {
				t.Fatal("expected malformed produce request to be rejected")
			}
		})
	}
}

func malformedFetchResponse(topicLen int16, topic string, recordCount int32, tail func(*bytes.Buffer)) *bytes.Reader {
	buf := new(bytes.Buffer)
	writeInt16(buf, topicLen)
	buf.WriteString(topic)
	writeInt32(buf, 0) // partition
	writeInt16(buf, 0) // error code
	writeInt64(buf, 0) // high watermark
	writeInt32(buf, recordCount)
	if tail != nil {
		tail(buf)
	}
	return bytes.NewReader(buf.Bytes())
}

func TestDecodeFetchResponseRejectsInvalidLengths(t *testing.T) {
	tests := []struct {
		name string
		body *bytes.Reader
	}{
		{
			name: "negative topic length",
			body: malformedFetchResponse(-1, "", 0, nil),
		},
		{
			name: "negative record count",
			body: malformedFetchResponse(1, "t", -1, nil),
		},
		{
			name: "record count exceeds remaining frame",
			body: malformedFetchResponse(1, "t", 1, nil),
		},
		{
			name: "key length below null sentinel",
			body: malformedFetchResponse(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt64(buf, 0)
				writeInt64(buf, 0)
				writeInt32(buf, -2)
			}),
		},
		{
			name: "key length exceeds remaining frame",
			body: malformedFetchResponse(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt64(buf, 0)
				writeInt64(buf, 0)
				writeInt32(buf, 100)
			}),
		},
		{
			name: "value length below null sentinel",
			body: malformedFetchResponse(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt64(buf, 0)
				writeInt64(buf, 0)
				writeInt32(buf, 0)
				writeInt32(buf, -2)
			}),
		},
		{
			name: "value length exceeds remaining frame",
			body: malformedFetchResponse(1, "t", 1, func(buf *bytes.Buffer) {
				writeInt64(buf, 0)
				writeInt64(buf, 0)
				writeInt32(buf, 0)
				writeInt32(buf, 100)
			}),
		},
	}

	codec := &WireCodec{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := codec.DecodeFetchResponseBody(tt.body); err == nil {
				t.Fatal("expected malformed fetch response to be rejected")
			}
		})
	}
}
