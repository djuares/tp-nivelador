package safe_socket

import (
	"io"
)

// SendAll writes all of data to w, looping over Write to tolerate short
// writes (a single call to Write is not guaranteed to write everything).
func SendAll(w io.Writer, data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		// Write puede devolver n=0 sin error, sin que eso signifique que
		// la conexión se cerró (a diferencia de un Read que devuelve 0
		// bytes junto con io.EOF); si eso pasa, simplemente hay que
		// reintentar.
		n, err := w.Write(data[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}

// RecvAll reads exactly size bytes from r, looping over Read to tolerate
// short reads (a single call to Read may return fewer bytes than
// requested, even before reaching EOF).
func RecvAll(r io.Reader, size int) ([]byte, error) {
	buf := make([]byte, size)
	totalRead := 0
	for totalRead < size {
		n, err := r.Read(buf[totalRead:])
		totalRead += n
		if err != nil {
			if err == io.EOF && totalRead == size {
				break
			}
			return nil, err
		}
	}
	return buf, nil
}
