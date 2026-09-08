package client

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)


const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)

	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func isShutdown(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() != nil
}

// encodeBetBatch reuses the supplied buffer instead of creating a new
// bytes.Buffer for every batch. The resulting payload has exactly the
// same format as protocol.EncodeBetBatch.
func encodeBetBatch(batch []bet.Bet, buffer []byte) []byte {
	buffer = buffer[:0]

	var tmp [4]byte

	// Number of bets.
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(batch)))
	buffer = append(buffer, count[:]...)

	for _, b := range batch {
		// Agency ID.
		binary.BigEndian.PutUint32(tmp[:], b.AgencyId)
		buffer = append(buffer, tmp[:]...)

		// First name.
		first := []byte(b.FirstName)
		buffer = append(buffer, byte(len(first)))
		buffer = append(buffer, first...)

		// Last name.
		last := []byte(b.LastName)
		buffer = append(buffer, byte(len(last)))
		buffer = append(buffer, last...)

		// Document.
		binary.BigEndian.PutUint32(tmp[:], b.Document)
		buffer = append(buffer, tmp[:]...)

		// Birthdate: exactly 10 bytes.
		var birthdate [10]byte
		copy(birthdate[:], b.Birthdate)
		buffer = append(buffer, birthdate[:]...)

		// Number.
		binary.BigEndian.PutUint32(tmp[:], b.Number)
		buffer = append(buffer, tmp[:]...)
	}

	return buffer
}

// documentFromCSVLine extracts the document (third CSV field) without
// parsing the complete Bet and without allocating a string.
func documentFromCSVLine(line []byte) (uint32, error) {
	field := 0
	start := 0

	for i := 0; i <= len(line); i++ {
		if i != len(line) && line[i] != ',' {
			continue
		}

		if field == 2 {
			if start == i {
				return 0, errors.New("invalid document: empty field")
			}

			var document uint32

			for j := start; j < i; j++ {
				c := line[j]

				if c < '0' || c > '9' {
					return 0, errors.New("invalid document")
				}

				document = document*10 + uint32(c-'0')
			}

			return document, nil
		}

		field++
		start = i + 1
	}

	return 0, errors.New("invalid bet line: missing document field")
}

func (client *Client) Run(ctx context.Context) error {
	// Keep the client's heap bounded while processing large input files.
	// The memory-profile test measures the container's peak RSS, so using
	// a more frequent GC prevents temporary parsing allocations from
	// causing the process to retain a much larger heap.
	oldGCPercent := debug.SetGCPercent(50)
	defer debug.SetGCPercent(oldGCPercent)

	const mainAction = "process-bets"

	defer client.conn.Close()

	shutdownWatcherDone := make(chan struct{})
	defer close(shutdownWatcherDone)

	go func() {
		select {
		case <-ctx.Done():
			logger.Info(
				"shutdown",
				logger.InProgress,
				"agency-id",
				client.config.AgencyId,
			)
			client.conn.Close()

		case <-shutdownWatcherDone:
		}
	}()

	agencyId64, err := strconv.ParseUint(client.config.AgencyId, 10, 32)
	if err != nil {
		logger.Error("parse-agency-id", logger.Fail, "err", err)
		return err
	}

	agencyId := uint32(agencyId64)

	batchSize, err := strconv.Atoi(client.config.BatchSize)
	if err != nil || batchSize <= 0 {
		logger.Error("parse-batch-size", logger.Fail, "err", err)
		return errors.New("BATCH_SIZE must be a positive integer")
	}

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "err", err)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "err", err)
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	logger.Info(
		mainAction,
		logger.InProgress,
		"agency-id",
		client.config.AgencyId,
	)

	// Reusable payload buffer. It grows at most to the size of one batch
	// and is reused for all subsequent batches.
	payloadBuffer := make([]byte, 0, batchSize*64)

	flushBatch := func(batch []bet.Bet) error {
		if len(batch) == 0 {
			return nil
		}

		payloadBuffer = encodeBetBatch(batch, payloadBuffer)

		if err := protocol.SendMessage(
			client.conn,
			protocol.BetBatch,
			payloadBuffer,
		); err != nil {
			return err
		}

		msgType, responsePayload, err := protocol.ReadMessage(client.conn)
		if err != nil {
			return err
		}

		if msgType != protocol.BatchAck {
			return errors.New("unexpected message type waiting for batch ack")
		}

		ok, err := protocol.DecodeAck(responsePayload)
		if err != nil {
			return err
		}

		if !ok {
			return errors.New("server rejected bet batch")
		}

		return nil
	}

	// First pass.
	scanner := bufio.NewScanner(inputFile)

	var batch []bet.Bet
	batch = make([]bet.Bet, 0, batchSize)

	batchesSent := 0

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")

		if line == "" {
			continue
		}

		parsedBet, err := bet.ParseBet(line, agencyId)
		if err != nil {
			logger.Error("parse-bet", logger.Fail, "err", err)
			return err
		}

		batch = append(batch, parsedBet)

		if len(batch) == batchSize {
			if err := flushBatch(batch); err != nil {
				if isShutdown(ctx, err) {
					logger.Info(
						"shutdown",
						logger.Success,
						"agency-id",
						client.config.AgencyId,
					)
					return nil
				}

				logger.Error(
					"send-batch",
					logger.Fail,
					"batch-id",
					batchesSent,
					"err",
					err,
				)
				return err
			}

			batchesSent++
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "err", err)
		return err
	}

	// Send remaining bets.
	if err := flushBatch(batch); err != nil {
		if isShutdown(ctx, err) {
			logger.Info(
				"shutdown",
				logger.Success,
				"agency-id",
				client.config.AgencyId,
			)
			return nil
		}

		logger.Error(
			"send-batch",
			logger.Fail,
			"batch-id",
			batchesSent,
			"err",
			err,
		)
		return err
	}

	if len(batch) > 0 {
		batchesSent++
	}

	// We no longer need the batch. Clearing the slice removes references
	// to the strings belonging to the last input lines.
	batch = nil

	// Tell the server there are no more bets.
	if err := protocol.SendMessage(
		client.conn,
		protocol.Finished,
		protocol.EncodeFinished(agencyId),
	); err != nil {
		if isShutdown(ctx, err) {
			logger.Info(
				"shutdown",
				logger.Success,
				"agency-id",
				client.config.AgencyId,
			)
			return nil
		}

		logger.Error("send-finished", logger.Fail, "err", err)
		return err
	}

	// Receive winners.
	msgType, responsePayload, err := protocol.ReadMessage(client.conn)
	if err != nil {
		if isShutdown(ctx, err) {
			logger.Info(
				"shutdown",
				logger.Success,
				"agency-id",
				client.config.AgencyId,
			)
			return nil
		}

		logger.Error("recv-winners", logger.Fail, "err", err)
		return err
	}

	if msgType != protocol.Winners {
		return errors.New("unexpected message type waiting for winners")
	}

	winningDocuments, err := protocol.DecodeWinners(responsePayload)
	if err != nil {
		logger.Error("decode-winners", logger.Fail, "err", err)
		return err
	}

	responsePayload = nil

	sort.Slice(winningDocuments, func(i, j int) bool {
		return winningDocuments[i] < winningDocuments[j]
	})

	// Second pass. We only extract the document from each line and write
	// the original line when it belongs to the winners set.
	if _, err := inputFile.Seek(0, io.SeekStart); err != nil {
		logger.Error("seek-input-file", logger.Fail, "err", err)
		return err
	}

	winnersWritten := 0
	winnersScanner := bufio.NewScanner(inputFile)

	for winnersScanner.Scan() {
		line := winnersScanner.Bytes()

		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}

		if len(line) == 0 {
			continue
		}

		document, err := documentFromCSVLine(line)
		if err != nil {
			logger.Error("parse-bet", logger.Fail, "err", err)
			return err
		}

		idx := sort.Search(
			len(winningDocuments),
			func(i int) bool {
				return winningDocuments[i] >= document
			},
		)

		if idx == len(winningDocuments) || winningDocuments[idx] != document {
			continue
		}

		if _, err := writer.Write(line); err != nil {
			logger.Error("write-output", logger.Fail, "err", err)
			return err
		}

		if err := writer.WriteByte('\n'); err != nil {
			logger.Error("write-output", logger.Fail, "err", err)
			return err
		}

		winnersWritten++
	}

	if err := winnersScanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "err", err)
		return err
	}

	if err := writer.Flush(); err != nil {
		logger.Error("flush-output-file", logger.Fail, "err", err)
		return err
	}

	logger.Info(
		mainAction,
		logger.Success,
		"agency-id",
		client.config.AgencyId,
		"batches-sent",
		batchesSent,
		"winners",
		winnersWritten,
	)

	return nil
}
