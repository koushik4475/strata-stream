package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Helper client encoding / decoding utilities

func (c *WireCodec) EncodeProduceHeader(buf *bytes.Buffer, correlationId int32) {
	_ = binary.Write(buf, binary.BigEndian, ApiKeyProduce)
	_ = binary.Write(buf, binary.BigEndian, int16(1))
	_ = binary.Write(buf, binary.BigEndian, correlationId)
	clientID := "strata-test-producer"
	_ = binary.Write(buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
}

func (c *WireCodec) AppendProducePayload(buf *bytes.Buffer, topic string, partition int32, messages []ProduceMessage) {
	_ = binary.Write(buf, binary.BigEndian, int16(len(topic)))
	buf.WriteString(topic)
	_ = binary.Write(buf, binary.BigEndian, partition)
	_ = binary.Write(buf, binary.BigEndian, int32(len(messages)))

	for _, m := range messages {
		_ = binary.Write(buf, binary.BigEndian, int32(len(m.Key)))
		if len(m.Key) > 0 {
			buf.Write(m.Key)
		}
		_ = binary.Write(buf, binary.BigEndian, int32(len(m.Value)))
		if len(m.Value) > 0 {
			buf.Write(m.Value)
		}
	}
}

func (c *WireCodec) DecodeProduceResponsePayload(frame []byte) (corrID int32, topic string, partition int32, baseOffset int64, errCode int16) {
	r := bytes.NewReader(frame)
	_ = binary.Read(r, binary.BigEndian, &corrID)

	var topicLen int16
	_ = binary.Read(r, binary.BigEndian, &topicLen)
	topicBytes := make([]byte, topicLen)
	_, _ = io.ReadFull(r, topicBytes)
	topic = string(topicBytes)

	_ = binary.Read(r, binary.BigEndian, &partition)
	_ = binary.Read(r, binary.BigEndian, &baseOffset)
	_ = binary.Read(r, binary.BigEndian, &errCode)
	return
}

func (c *WireCodec) EncodeFetchHeader(buf *bytes.Buffer, correlationId int32) {
	_ = binary.Write(buf, binary.BigEndian, ApiKeyFetch)
	_ = binary.Write(buf, binary.BigEndian, int16(1))
	_ = binary.Write(buf, binary.BigEndian, correlationId)
	clientID := "strata-test-consumer"
	_ = binary.Write(buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
}

func (c *WireCodec) AppendFetchPayload(buf *bytes.Buffer, topic string, partition int32, fetchOffset int64, maxBytes int32) {
	_ = binary.Write(buf, binary.BigEndian, int16(len(topic)))
	buf.WriteString(topic)
	_ = binary.Write(buf, binary.BigEndian, partition)
	_ = binary.Write(buf, binary.BigEndian, fetchOffset)
	_ = binary.Write(buf, binary.BigEndian, maxBytes)
}

func (c *WireCodec) DecodeResponseHeader(r *bytes.Reader, corrID *int32) error {
	return binary.Read(r, binary.BigEndian, corrID)
}

func (c *WireCodec) DecodeFetchResponseBody(r *bytes.Reader) (*FetchResponse, error) {
	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	if topicLen < 0 || int(topicLen) > r.Len() {
		return nil, errors.New("invalid topic length")
	}
	topicBytes := make([]byte, int(topicLen))
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, err
	}
	topic := string(topicBytes)

	var partition int32
	var errCode int16
	var hwm int64
	if err := binary.Read(r, binary.BigEndian, &partition); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &errCode); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &hwm); err != nil {
		return nil, err
	}

	var numRecords int32
	if err := binary.Read(r, binary.BigEndian, &numRecords); err != nil {
		return nil, err
	}
	// A record needs at least offset, timestamp, key length, and value length.
	if numRecords < 0 || int64(numRecords) > int64(r.Len()/24) {
		return nil, errors.New("invalid fetch record count")
	}

	records := make([]FetchedRecord, 0, int(numRecords))
	for i := int32(0); i < numRecords; i++ {
		var offset, ts int64
		if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
			return nil, err
		}

		var keyLen int32
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		if keyLen < -1 || int64(keyLen) > int64(r.Len()) {
			return nil, errors.New("invalid fetch key length")
		}
		var key []byte
		if keyLen > 0 {
			key = make([]byte, keyLen)
			if _, err := io.ReadFull(r, key); err != nil {
				return nil, err
			}
		}

		var valLen int32
		if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}
		if valLen < -1 || int64(valLen) > int64(r.Len()) {
			return nil, errors.New("invalid fetch value length")
		}
		var val []byte
		if valLen > 0 {
			val = make([]byte, valLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return nil, err
			}
		}

		records = append(records, FetchedRecord{
			Offset:    offset,
			Timestamp: ts,
			Key:       key,
			Value:     val,
		})
	}

	return &FetchResponse{
		Topic:         topic,
		Partition:     partition,
		ErrorCode:     errCode,
		HighWatermark: hwm,
		Records:       records,
	}, nil
}
