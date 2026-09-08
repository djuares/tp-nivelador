package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

// Message types
const (
	BetBatch byte = 0x01 // client -> server: N bets
	BatchAck byte = 0x02 // server -> client: 1 byte status
	Finished byte = 0x03 // client -> server: agency id, no more bets to send
	Winners  byte = 0x04 // server -> client: N winning documents
)

const (
	StatusOk    byte = 0x00
	StatusError byte = 0x01
)

// Every message on the wire is: [1 byte type][4 bytes big-endian length][payload]
const headerSize = 5
const birthdateSize = 10 // "YYYY-MM-DD"

// SendMessage writes a full [type][length][payload] framed message.
func SendMessage(w io.Writer, msgType byte, payload []byte) error {
	header := make([]byte, headerSize)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	if err := safe_socket.SendAll(w, header); err != nil {
		return err
	}
	if len(payload) > 0 {
		return safe_socket.SendAll(w, payload)
	}
	return nil
}

// ReadMessage reads a full framed message and returns its type and payload.
func ReadMessage(r io.Reader) (byte, []byte, error) {
	header, err := safe_socket.RecvAll(r, headerSize)
	if err != nil {
		return 0, nil, err
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:])
	if length == 0 {
		return msgType, nil, nil
	}

	payload, err := safe_socket.RecvAll(r, int(length))
	if err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// EncodeBetBatch serializes a slice of bets into a BET_BATCH payload.
func EncodeBetBatch(bets []bet.Bet) []byte {
	size := 2

	for _, b := range bets {
		size += 4                         // agency id
		size += 1 + len(b.FirstName)     // first name
		size += 1 + len(b.LastName)      // last name
		size += 4                         // document
		size += birthdateSize             // birthdate
		size += 4                         // number
	}

	payload := make([]byte, size)

	offset := 0

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(bets)))
	offset += 2

	for _, b := range bets {
		binary.BigEndian.PutUint32(payload[offset:], b.AgencyId)
		offset += 4

		first := []byte(b.FirstName)
		payload[offset] = byte(len(first))
		offset++
		copy(payload[offset:], first)
		offset += len(first)

		last := []byte(b.LastName)
		payload[offset] = byte(len(last))
		offset++
		copy(payload[offset:], last)
		offset += len(last)

		binary.BigEndian.PutUint32(payload[offset:], b.Document)
		offset += 4

		copy(payload[offset:offset+birthdateSize], b.Birthdate)
		offset += birthdateSize

		binary.BigEndian.PutUint32(payload[offset:], b.Number)
		offset += 4
	}

	return payload
}

// EncodeFinished serializes a FINISHED payload (just the agency id).
func EncodeFinished(agencyId uint32) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, agencyId)
	return buf.Bytes()
}

// DecodeAck parses a BATCH_ACK payload.
func DecodeAck(payload []byte) (bool, error) {
	if len(payload) != 1 {
		return false, errors.New("invalid ack payload")
	}
	return payload[0] == StatusOk, nil
}

// DecodeWinners parses a WINNERS payload into a set of winning documents.
func DecodeWinners(payload []byte) ([]uint32, error) {
	if len(payload) < 2 {
		return nil, errors.New("invalid winners payload")
	}

	count := binary.BigEndian.Uint16(payload[:2])
	offset := 2

	if offset+int(count)*4 > len(payload) {
		return nil, errors.New("invalid winners payload")
	}

	winners := make([]uint32, count)

	for i := 0; i < int(count); i++ {
		winners[i] = binary.BigEndian.Uint32(payload[offset:])
		offset += 4
	}

	return winners, nil
}

