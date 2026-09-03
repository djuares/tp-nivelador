import socket


def send_all(sock: socket.socket, data: bytes) -> None:
    """Sends all bytes in data, looping over sock.send to tolerate short
    writes (a single call to send() is not guaranteed to send everything)."""
    total_sent = 0
    while total_sent < len(data):
        # send() puede devolver 0 sin que la conexión esté cerrada (a
        # diferencia de recv()); si eso pasa, simplemente hay que
        # reintentar. Una conexión realmente rota levanta una excepción
        # (BrokenPipeError/ConnectionResetError), no devuelve 0.
        sent = sock.send(data[total_sent:])
        total_sent += sent


def recv_all(sock: socket.socket, size: int) -> bytes:
    """Receives exactly `size` bytes, looping over sock.recv to tolerate
    short reads (a single call to recv() may return fewer bytes than
    requested)."""
    chunks = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("connection closed while receiving")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
