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
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint16(len(bets)))

	for _, b := range bets {
		binary.Write(buf, binary.BigEndian, b.AgencyId)

		first := []byte(b.FirstName)
		buf.WriteByte(byte(len(first)))
		buf.Write(first)

		last := []byte(b.LastName)
		buf.WriteByte(byte(len(last)))
		buf.Write(last)

		binary.Write(buf, binary.BigEndian, b.Document)

		birthdate := make([]byte, birthdateSize)
		copy(birthdate, b.Birthdate)
		buf.Write(birthdate)

		binary.Write(buf, binary.BigEndian, b.Number)
	}

	return buf.Bytes()
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
func DecodeWinners(payload []byte) (map[uint32]bool, error) {
	if len(payload) < 2 {
		return nil, errors.New("invalid winners payload")
	}
	count := binary.BigEndian.Uint16(payload[:2])
	offset := 2

	winners := make(map[uint32]bool, count)
	for i := 0; i < int(count); i++ {
		if offset+4 > len(payload) {
			return nil, errors.New("invalid winners payload")
		}
		document := binary.BigEndian.Uint32(payload[offset : offset+4])
		winners[document] = true
		offset += 4
	}
	return winners, nil
}
