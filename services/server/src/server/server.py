import socket
import threading

import logger
import protocol
from lottery import Bet, Lottery

_STORAGE_PATH = "bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path=_STORAGE_PATH)

        # Protects every read/write to the shared bets storage, so
        # concurrent client threads never interleave a torn write or
        # read a half-written line.
        self._storage_lock = threading.Lock()

        # Coordinates the draw: client threads block on `_quorum_cv`
        # after sending FINISHED until at least `agency_quorum_min`
        # distinct agencies have notified they are done.
        self._agency_quorum_min = agency_quorum_min
        self._quorum_cv = threading.Condition()
        self._finished_agencies: set[int] = set()

    @staticmethod
    def _to_domain_bet(dto: protocol.BetDTO) -> Bet:
        return Bet(
            dto.agency_id,
            dto.first_name,
            dto.last_name,
            dto.document,
            dto.birthdate,
            dto.number,
        )

    def _handle_bet_batch(self, client_socket, payload: bytes):
        bets = protocol.decode_bets(payload)
        with self._storage_lock:
            self.lottery.store_bets([self._to_domain_bet(b) for b in bets])
        protocol.send_message(client_socket, protocol.BATCH_ACK, protocol.encode_ack(True))
        return bets[0].agency_id if bets else None

    def _await_quorum(self, agency_id: int) -> None:
        """Registers `agency_id` as finished and blocks the calling thread
        until at least `_agency_quorum_min` distinct agencies have
        finished. The thread that completes the quorum wakes up every
        other thread waiting here."""
        with self._quorum_cv:
            self._finished_agencies.add(agency_id)
            if len(self._finished_agencies) >= self._agency_quorum_min:
                self._quorum_cv.notify_all()
            else:
                self._quorum_cv.wait_for(
                    lambda: len(self._finished_agencies) >= self._agency_quorum_min
                )

    def _handle_finished(self, client_socket, payload: bytes) -> None:
        agency_id = protocol.decode_finished(payload)

        self._await_quorum(agency_id)

        # The draw itself only requires reading the bets already stored
        # for this agency, which are guaranteed to be persisted by the
        # time this agency sent FINISHED. We only ever answer this
        # agency's own winners -- never broadcast everyone's winners to
        # every client.
        with self._storage_lock:
            bets = list(self.lottery.load_bets())

        winning_documents = [
            bet.document
            for bet in bets
            if bet.agency_id == agency_id and self.lottery.has_won(bet)
        ]
        protocol.send_message(
            client_socket, protocol.WINNERS, protocol.encode_winners(winning_documents)
        )

    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        batches_received = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                msg_type, payload = protocol.read_message(client_socket)

                if msg_type == protocol.BET_BATCH:
                    batch_agency_id = self._handle_bet_batch(client_socket, payload)
                    agency_id = agency_id or batch_agency_id
                    batches_received += 1

                elif msg_type == protocol.FINISHED:
                    self._handle_finished(client_socket, payload)
                    break

                else:
                    raise ValueError(f"unexpected message type: {msg_type}")

            logger.info(
                action,
                logger.LogResult.success,
                "agency-id",
                agency_id,
                "batches-received",
                batches_received,
            )
        except Exception as e:
            logger.error(action, logger.LogResult.fail, "agency-id", agency_id)
            raise e
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            threads: list[threading.Thread] = []
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                # Each accepted connection is handled on its own thread so
                # the server can accept and process multiple agencies
                # concurrently instead of serving them one at a time.
                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                )
                client_thread.start()
                threads.append(client_thread)
