import signal
import socket
import threading

import logger
import protocol
from lottery import Bet, Lottery

_STORAGE_PATH = "bets.csv"

# How long the main thread waits for in-flight client handler threads to
# notice the shutdown and finish cleaning up their own resources before
# giving up on them (they are daemon threads, so the process will
# terminate regardless once `run` returns).
_SHUTDOWN_JOIN_TIMEOUT_SECONDS = 2


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

        # Set by the SIGTERM handler (which always runs on the main
        # thread) so every other thread can notice a shutdown was
        # requested and unwind instead of blocking forever.
        self._shutdown_requested = threading.Event()
        self._server_socket: socket.socket | None = None

        # Tracks every socket currently attached to a client handler
        # thread, so a shutdown can force-close them and unblock any
        # thread stuck in `recv`.
        self._clients_lock = threading.Lock()
        self._client_sockets: set[socket.socket] = set()

    def _register_client_socket(self, client_socket) -> None:
        with self._clients_lock:
            self._client_sockets.add(client_socket)

    def _unregister_client_socket(self, client_socket) -> None:
        with self._clients_lock:
            self._client_sockets.discard(client_socket)

    def _handle_sigterm(self, signum, frame) -> None:
        logger.info("shutdown", logger.LogResult.in_progress, "signal", "SIGTERM")
        self._shutdown_requested.set()

        # Unblocks the `accept()` call in the main thread: since Python
        # automatically retries syscalls interrupted by a signal whose
        # handler returns normally (PEP 475), we close the underlying
        # fd here so the retried `accept()` fails immediately instead
        # of blocking again.
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass

        # Wakes up any thread blocked waiting for the agency quorum, so
        # it can notice the shutdown instead of hanging until every
        # agency finishes (which may never happen).
        with self._quorum_cv:
            self._quorum_cv.notify_all()

        # Force-closing every connected client socket unblocks any
        # thread currently blocked on `recv`, making it raise right
        # away instead of waiting for data that will never arrive.
        with self._clients_lock:
            for client_socket in self._client_sockets:
                try:
                    client_socket.close()
                except OSError:
                    pass

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
            quorum_met = len(self._finished_agencies) >= self._agency_quorum_min
            if quorum_met or self._shutdown_requested.is_set():
                self._quorum_cv.notify_all()
            else:
                self._quorum_cv.wait_for(
                    lambda: len(self._finished_agencies) >= self._agency_quorum_min
                    or self._shutdown_requested.is_set()
                )

        if self._shutdown_requested.is_set():
            raise ConnectionAbortedError("server is shutting down")

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
        self._register_client_socket(client_socket)
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
            if self._shutdown_requested.is_set():
                # The connection was closed on purpose as part of a
                # graceful shutdown, not a real communication failure.
                logger.info(action, "shutdown", "agency-id", agency_id)
            else:
                logger.error(action, logger.LogResult.fail, "agency-id", agency_id)
                raise e
        finally:
            self._unregister_client_socket(client_socket)
            client_socket.close()

    def run(self):
        action = "accept-connection"
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self._server_socket = server_socket
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            threads: list[threading.Thread] = []
            while not self._shutdown_requested.is_set():
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except OSError:
                    if self._shutdown_requested.is_set():
                        break
                    logger.error(action, logger.LogResult.fail)
                    raise
                logger.info(action, logger.LogResult.success)

                # Each accepted connection is handled on its own thread so
                # the server can accept and process multiple agencies
                # concurrently instead of serving them one at a time.
                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                )
                client_thread.start()
                threads.append(client_thread)

        if self._shutdown_requested.is_set():
            # Give in-flight handlers a bounded window to notice their
            # socket was closed and unwind through their own cleanup
            # (`finally` blocks) before the process terminates. They
            # are daemon threads, so this is best-effort: the process
            # will terminate on its own once `run` returns either way.
            for client_thread in threads:
                client_thread.join(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECONDS)
            logger.info("shutdown", logger.LogResult.success)
