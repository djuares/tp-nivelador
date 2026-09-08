package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
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

// isShutdown reports whether err is (most likely) the side effect of the
// connection being force-closed by the shutdown watcher goroutine rather
// than a genuine communication failure.
func isShutdown(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() != nil
}

// Run reads the bets file (INPUT_FILE) line by line, groups them into
// batches of size BATCH_SIZE and sends them to the server using the
// binary protocol (BET_BATCH). Once the file is finished, it notifies
// FINISHED, waits for the list of winning documents (WINNERS) and dumps
// the original lines of the winning bets into OUTPUT_FILE.
func (client *Client) Run(ctx context.Context) error {
	const mainAction = "process-bets"
	defer client.conn.Close()

	// If a shutdown is requested while the client is blocked reading or
	// writing to the connection, closing it here makes that blocked
	// call return immediately with an error instead of waiting until a
	// response arrives that may never come.
	shutdownWatcherDone := make(chan struct{})
	defer close(shutdownWatcherDone)
	go func() {
		select {
		case <-ctx.Done():
			logger.Info("shutdown", logger.InProgress, "agency-id", client.config.AgencyId)
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

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	// Stores the original line of each sent bet, indexed by document,
	// so the line can be dumped as-is to OUTPUT_FILE if it turns out to
	// be a winner, without having to re-serialize it.
	sentBets := make(map[uint32]string)

	// flushBatch sends the accumulated batch as a single BET_BATCH
	// message and waits for its BATCH_ACK before continuing to read the
	// file.
	flushBatch := func(batch []bet.Bet) error {
		if len(batch) == 0 {
			return nil
		}

		if err := protocol.SendMessage(client.conn, protocol.BetBatch, protocol.EncodeBetBatch(batch)); err != nil {
			return err
		}

		msgType, payload, err := protocol.ReadMessage(client.conn)
		if err != nil {
			return err
		}
		if msgType != protocol.BatchAck {
			return errors.New("unexpected message type waiting for batch ack")
		}

		ok, err := protocol.DecodeAck(payload)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("server rejected bet batch")
		}
		return nil
	}

	scanner := bufio.NewScanner(inputFile)
	var batch []bet.Bet
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

		sentBets[parsedBet.Document] = line
		batch = append(batch, parsedBet)

		if len(batch) == batchSize {
			if err := flushBatch(batch); err != nil {
				if isShutdown(ctx, err) {
					logger.Info("shutdown", logger.Success, "agency-id", client.config.AgencyId)
					return nil
				}
				logger.Error("send-batch", logger.Fail, "batch-id", batchesSent, "err", err)
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

	// Sends the remaining bets that did not complete a full batch.
	if err := flushBatch(batch); err != nil {
		if isShutdown(ctx, err) {
			logger.Info("shutdown", logger.Success, "agency-id", client.config.AgencyId)
			return nil
		}
		logger.Error("send-batch", logger.Fail, "batch-id", batchesSent, "err", err)
		return err
	}
	if len(batch) > 0 {
		batchesSent++
	}

	// Notifies that there are no more bets for this agency and waits
	// for the list of winning documents.
	if err := protocol.SendMessage(client.conn, protocol.Finished, protocol.EncodeFinished(agencyId)); err != nil {
		if isShutdown(ctx, err) {
			logger.Info("shutdown", logger.Success, "agency-id", client.config.AgencyId)
			return nil
		}
		logger.Error("send-finished", logger.Fail, "err", err)
		return err
	}

	msgType, payload, err := protocol.ReadMessage(client.conn)
	if err != nil {
		if isShutdown(ctx, err) {
			logger.Info("shutdown", logger.Success, "agency-id", client.config.AgencyId)
			return nil
		}
		logger.Error("recv-winners", logger.Fail, "err", err)
		return err
	}
	if msgType != protocol.Winners {
		return errors.New("unexpected message type waiting for winners")
	}

	winningDocuments, err := protocol.DecodeWinners(payload)
	if err != nil {
		logger.Error("decode-winners", logger.Fail, "err", err)
		return err
	}

	winnersWritten := 0
	for document, line := range sentBets {
		if !winningDocuments[document] {
			continue
		}
		if _, err := writer.WriteString(line); err != nil {
			logger.Error("write-output", logger.Fail, "err", err)
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			logger.Error("write-output", logger.Fail, "err", err)
			return err
		}
		winnersWritten++
	}

	if err := writer.Flush(); err != nil {
		logger.Error("flush-output-file", logger.Fail, "err", err)
		return err
	}

	logger.Info(
		mainAction, logger.Success,
		"agency-id", client.config.AgencyId,
		"batches-sent", batchesSent,
		"winners", winnersWritten,
	)

	return nil
}