import struct
from dataclasses import dataclass

import safe_socket

# --- Message types ---
BET_BATCH = 0x01  # client -> server: N bets
BATCH_ACK = 0x02  # server -> client: 1 byte status
FINISHED = 0x03  # client -> server: agency id, no more bets to send
WINNERS = 0x04  # server -> client: N winning documents

_STATUS_OK = 0x00
_STATUS_ERROR = 0x01

# Every message on the wire is: [1 byte type][4 bytes big-endian length][payload]
_HEADER_FORMAT = ">BI"
_HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)

_BIRTHDATE_SIZE = 10  # "YYYY-MM-DD"


@dataclass
class BetDTO:
    """Wire-level representation of a bet. Deliberately separate from the
    domain `Bet` class in `lottery`: this layer only knows how to turn
    bytes into plain data, translating it into a domain object is the
    caller's (server's) responsibility."""

    agency_id: int
    first_name: str
    last_name: str
    document: int
    birthdate: str
    number: int


# --- Framing: read/write a full [type][length][payload] message ---


def read_message(sock) -> tuple[int, bytes]:
    header = safe_socket.recv_all(sock, _HEADER_SIZE)
    msg_type, length = struct.unpack(_HEADER_FORMAT, header)
    payload = safe_socket.recv_all(sock, length) if length > 0 else b""
    return msg_type, payload


def send_message(sock, msg_type: int, payload: bytes) -> None:
    header = struct.pack(_HEADER_FORMAT, msg_type, len(payload))
    safe_socket.send_all(sock, header + payload)


# --- BET_BATCH decoding ---


def decode_bets(payload: bytes) -> list[BetDTO]:
    (count,) = struct.unpack_from(">H", payload, 0)
    offset = 2
    bets = []
    for _ in range(count):
        bet, offset = _decode_bet(payload, offset)
        bets.append(bet)
    return bets


def _decode_bet(data: bytes, offset: int) -> tuple[BetDTO, int]:
    (agency_id,) = struct.unpack_from(">I", data, offset)
    offset += 4

    (first_len,) = struct.unpack_from(">B", data, offset)
    offset += 1
    first_name = data[offset : offset + first_len].decode("utf-8")
    offset += first_len

    (last_len,) = struct.unpack_from(">B", data, offset)
    offset += 1
    last_name = data[offset : offset + last_len].decode("utf-8")
    offset += last_len

    (document,) = struct.unpack_from(">I", data, offset)
    offset += 4

    birthdate = data[offset : offset + _BIRTHDATE_SIZE].decode("utf-8")
    offset += _BIRTHDATE_SIZE

    (number,) = struct.unpack_from(">I", data, offset)
    offset += 4

    return (
        BetDTO(agency_id, first_name, last_name, document, birthdate, number),
        offset,
    )


# --- FINISHED decoding ---


def decode_finished(payload: bytes) -> int:
    (agency_id,) = struct.unpack_from(">I", payload, 0)
    return agency_id


# --- Responses encoding ---


def encode_ack(ok: bool) -> bytes:
    return struct.pack(">B", _STATUS_OK if ok else _STATUS_ERROR)


def encode_winners(documents: list[int]) -> bytes:
    payload = struct.pack(">H", len(documents))
    for document in documents:
        payload += struct.pack(">I", document)
    return payload
