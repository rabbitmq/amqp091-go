package amqp091

import (
	"io"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

func TestDialTLSConfigFailureClosesSocket(t *testing.T) {
	for _, mode := range []string{"cacertfile", "client-keypair"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			peers := make(chan net.Conn, 1)
			go func() {
				p, e := listener.Accept()
				if e == nil {
					peers <- p
				}
			}()
			missing := filepath.Join(t.TempDir(), "missing.pem")
			q := url.Values{}
			if mode == "cacertfile" {
				q.Set("cacertfile", missing)
			} else {
				q.Set("certfile", missing)
				q.Set("keyfile", missing)
			}
			_, err = Dial("amqps://" + listener.Addr().String() + "/?" + q.Encode())
			if err == nil {
				t.Fatal("expected certificate error")
			}
			select {
			case peer := <-peers:
				defer peer.Close()
				peer.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
				_, err = peer.Read(make([]byte, 1))
				if err != io.EOF {
					t.Errorf("Dial returned certificate error but socket still open: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("no connection accepted")
			}
		})
	}
}
