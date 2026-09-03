import socket

import logger
import protocol
from lottery import Bet, Lottery

_STORAGE_PATH = "bets.csv"


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path=_STORAGE_PATH)

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
        self.lottery.store_bets([self._to_domain_bet(b) for b in bets])
        protocol.send_message(client_socket, protocol.BATCH_ACK, protocol.encode_ack(True))
        return bets[0].agency_id if bets else None

    def _handle_finished(self, client_socket, payload: bytes) -> None:
        agency_id = protocol.decode_finished(payload)
        winning_documents = [
            bet.document
            for bet in self.lottery.load_bets()
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

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
