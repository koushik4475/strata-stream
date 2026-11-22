package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// Standard Kafka API Keys
const (
	ApiKeyProduce      int16 = 0
	ApiKeyFetch        int16 = 1
	ApiKeyListOffsets  int16 = 2
	ApiKeyMetadata     int16 = 3
	ApiKeyApiVersions  int16 = 18
)

// RequestHeader represents the standard Kafka protocol request header.
type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

// ProduceMessage represents an individual record within a produce request.
type ProduceMessage struct {
	Key   []byte
	Value []byte
}

// ProduceRequest models a simplified Kafka produce batch.
type ProduceRequest struct {
	Topic     string
	Partition int32
	Messages  []ProduceMessage
}

// ProduceResponse models Kafka produce reply.
type ProduceResponse struct {
	CorrelationId int32
	Topic         string
	Partition     int32
	BaseOffset    int64
	ErrorCode     int16
}

// FetchRequest models a consumer fetch request.
type FetchRequest struct {
	Topic       string
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

// FetchResponse models consumer fetch reply with returned records.
type FetchResponse struct {
	CorrelationId int32
	Topic         string
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	Records       []FetchedRecord
}

// FetchedRecord represents returned record payload.
type FetchedRecord struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// WireCodec parses and encodes StrataStream wire protocol frames.
type WireCodec struct{}

// ReadFrame reads a size-delimited frame: [4-byte big-endian size] + [payload]
func (c *WireCodec) ReadFrame(r io.Reader) ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	if size > 64*1024*1024 { // 64MB protection limit
		return nil, errors.New("frame exceeds maximum allowed size (64MB)")
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// WriteFrame sends a size-delimited frame over the connection.
func (c *WireCodec) WriteFrame(w io.Writer, payload []byte) error {
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(payload)))
	if _, err := w.Write(sizeBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// DecodeRequestHeader parses Kafka Request Header v1.
func (c *WireCodec) DecodeRequestHeader(r *bytes.Reader) (*RequestHeader, error) {
	var apiKey, apiVersion int16
	var correlationId int32
	if err := binary.Read(r, binary.BigEndian, &apiKey); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &apiVersion); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &correlationId); err != nil {
		return nil, err
	}

	var clientIDLen int16
	if err := binary.Read(r, binary.BigEndian, &clientIDLen); err != nil {
		return nil, err
	}

	if clientIDLen < -1 || int(clientIDLen) > r.Len() {
		return nil, errors.New("invalid client ID length")
	}

	var clientID string
	if clientIDLen > 0 {
		cidBytes := make([]byte, clientIDLen)
		if _, err := io.ReadFull(r, cidBytes); err != nil {
			return nil, err
		}
		clientID = string(cidBytes)
	}

	return &RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationId: correlationId,
		ClientId:      clientID,
	}, nil
}

// EncodeResponseHeader encodes standard response header [CorrelationId int32].
func (c *WireCodec) EncodeResponseHeader(w io.Writer, correlationId int32) error {
	return binary.Write(w, binary.BigEndian, correlationId)
}

// DecodeProduceRequest parses a produce batch.
func (c *WireCodec) DecodeProduceRequest(r *bytes.Reader) (*ProduceRequest, error) {
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
	if err := binary.Read(r, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	var numMsgs int32
	if err := binary.Read(r, binary.BigEndian, &numMsgs); err != nil {
		return nil, err
	}
	// Every message needs at least two int32 length prefixes. Bound the
	// allocation by the bytes remaining in this already size-limited frame.
	if numMsgs < 0 || int64(numMsgs) > int64(r.Len()/8) {
		return nil, errors.New("invalid produce message count")
	}

	messages := make([]ProduceMessage, 0, int(numMsgs))
	for i := int32(0); i < numMsgs; i++ {
		var keyLen int32
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		if keyLen < -1 || int64(keyLen) > int64(r.Len()) {
			return nil, errors.New("invalid produce key length")
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
			return nil, errors.New("invalid produce value length")
		}
		var val []byte
		if valLen > 0 {
			val = make([]byte, valLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return nil, err
			}
		}

		messages = append(messages, ProduceMessage{
			Key:   key,
			Value: val,
		})
	}

	return &ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  messages,
	}, nil
}

// EncodeProduceResponse encodes ProduceResponse into bytes.
func (c *WireCodec) EncodeProduceResponse(resp *ProduceResponse) []byte {
	buf := new(bytes.Buffer)
	_ = c.EncodeResponseHeader(buf, resp.CorrelationId)

	_ = binary.Write(buf, binary.BigEndian, int16(len(resp.Topic)))
	buf.WriteString(resp.Topic)
	_ = binary.Write(buf, binary.BigEndian, resp.Partition)
	_ = binary.Write(buf, binary.BigEndian, resp.BaseOffset)
	_ = binary.Write(buf, binary.BigEndian, resp.ErrorCode)

	return buf.Bytes()
}

// DecodeFetchRequest parses a fetch request.
func (c *WireCodec) DecodeFetchRequest(r *bytes.Reader) (*FetchRequest, error) {
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
	var fetchOffset int64
	var maxBytes int32
	if err := binary.Read(r, binary.BigEndian, &partition); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &fetchOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &maxBytes); err != nil {
		return nil, err
	}

	return &FetchRequest{
		Topic:       topic,
		Partition:   partition,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
	}, nil
}

// EncodeFetchResponse encodes FetchResponse into bytes.
func (c *WireCodec) EncodeFetchResponse(resp *FetchResponse) []byte {
	buf := new(bytes.Buffer)
	_ = c.EncodeResponseHeader(buf, resp.CorrelationId)

	_ = binary.Write(buf, binary.BigEndian, int16(len(resp.Topic)))
	buf.WriteString(resp.Topic)
	_ = binary.Write(buf, binary.BigEndian, resp.Partition)
	_ = binary.Write(buf, binary.BigEndian, resp.ErrorCode)
	_ = binary.Write(buf, binary.BigEndian, resp.HighWatermark)

	_ = binary.Write(buf, binary.BigEndian, int32(len(resp.Records)))
	for _, rec := range resp.Records {
		_ = binary.Write(buf, binary.BigEndian, rec.Offset)
		_ = binary.Write(buf, binary.BigEndian, rec.Timestamp)

		keyLen := int32(len(rec.Key))
		if rec.Key == nil {
			keyLen = -1
		}
		_ = binary.Write(buf, binary.BigEndian, keyLen)
		if keyLen > 0 {
			buf.Write(rec.Key)
		}

		valLen := int32(len(rec.Value))
		_ = binary.Write(buf, binary.BigEndian, valLen)
		if valLen > 0 {
			buf.Write(rec.Value)
		}
	}

	return buf.Bytes()
}
