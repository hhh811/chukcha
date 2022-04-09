package integration

import (
	"chukcha/client"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024

	sendFmt = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

func TestSimpleClientAndServeConcurrently(t *testing.T) {
	t.Parallel()
	simpleClientAndServerTest(t, true)
}

func TestSimpleClientAndServeSequantially(t *testing.T) {
	t.Parallel()
	simpleClientAndServerTest(t, false)
}

func simpleClientAndServerTest(t *testing.T, concurrent bool) {
	t.Helper()

	log.SetFlags(log.Flags() | log.Lmicroseconds)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}

	dbPath, err := os.MkdirTemp(os.TempDir(), "chukcha")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	t.Cleanup(func() { os.RemoveAll(dbPath) })

	categoryPath := filepath.Join(dbPath, "numbers")
	os.MkdirAll(categoryPath, 0777)

	ioutil.WriteFile(filepath.Join(categoryPath, fmt.Sprintf("hc-chunk%09d", 1)), []byte("12345\n"), 0666)

	// start chukcha
	log.Printf("Running chukcha on port %d", port)
	errCh := make(chan error, 1)
	go func() {
		errCh <- InitAndServe(fmt.Sprintf("http://localhost:%d/", 2379), "hc", dbPath, fmt.Sprintf("localhost:%d", port))
	}()
	log.Printf("Waiting for the Chukcha port localhost:%d to open", port)
	waitForPort(t, port, errCh)

	log.Printf("Starting the test")

	s := client.NewSimple([]string{fmt.Sprintf("http://localhost:%d", port)})

	var want, got int64

	if concurrent {
		want, got, err = sendAndReceiveConcurrently(s)
	} else {
		want, err = send(s)
		if err != nil {
			t.Fatalf("send error: %v", err)
		}

		sendFinishedCh := make(chan bool, 1)
		sendFinishedCh <- true
		got, err = receive(s, sendFinishedCh)
		if err != nil {
			t.Fatalf("receive error: %v", err)
		}
	}

	if err != nil {
		t.Errorf("sendAndReceiveConcurrently failed: %v", err)
	}

	want += 12345

	if want != got {
		t.Errorf("the expected sum %d is not equal to the actual sum %d", want, got)
	}
}

func waitForPort(t *testing.T, port int, errCh chan error) {
	t.Helper()

	for i := 0; i <= 100; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("InitAndServe failed: %v", err)
			}
		default:
		}

		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}
}

type sumAndErr struct {
	sum int64
	err error
}

func sendAndReceiveConcurrently(s *client.Simple) (want, got int64, err error) {
	wantCh := make(chan sumAndErr, 1)
	gotCh := make(chan sumAndErr, 1)
	sendFinishedCh := make(chan bool, 1)

	go func() {
		want, err := send(s)
		wantCh <- sumAndErr{
			sum: want,
			err: err,
		}
		sendFinishedCh <- true
	}()

	go func() {
		got, err := receive(s, sendFinishedCh)
		gotCh <- sumAndErr{
			sum: got,
			err: err,
		}
	}()

	wantRes := <-wantCh
	if wantRes.err != nil {
		return 0, 0, fmt.Errorf("send error: %v", wantRes.err)
	}

	gotRes := <-gotCh
	if gotRes.err != nil {
		return 0, 0, fmt.Errorf("receive error: %v", gotRes.err)
	}

	return wantRes.sum, gotRes.sum, err
}

func send(s *client.Simple) (sum int64, err error) {
	sendStart := time.Now()
	var networkTime time.Duration
	var sendBytes int

	defer func() {
		log.Printf(sendFmt, networkTime, time.Since(sendStart)-networkTime, float64(sendBytes)/1024/1024)
	}()

	buf := make([]byte, 0, maxBufferSize)

	for i := 0; i <= maxN; i++ {
		sum += int64(i)

		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, '\n')

		if len(buf) >= maxBufferSize {
			start := time.Now()
			if err := s.Send("numbers", buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sendBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send("numbers", buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sendBytes += len(buf)
	}

	return sum, nil
}

var randomTempErr = errors.New("a random temporary error occurred")

func receive(s *client.Simple, sendFinishedCh chan bool) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()

	trimNL := func(r rune) bool {
		return r == '\n'
	}

	sendFinished := false

	loopCnt := 0

	for {
		loopCnt++

		select {
		case <-sendFinishedCh:
			log.Printf("Receive: got information that send finished")
			sendFinished = true
		default:
		}

		err := s.Process("numbers", buf, func(res []byte) error {
			if loopCnt%10 == 0 {
				return randomTempErr
			}

			start := time.Now()

			ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
			for _, str := range ints {
				i, err := strconv.Atoi(str)
				if err != nil {
					return err
				}
				sum += int64(i)
			}

			parseTime += time.Since(start)
			return nil
		})

		if errors.Is(err, randomTempErr) {
			continue
		} else if errors.Is(err, io.EOF) {
			if sendFinished {
				return sum, nil
			}
			time.Sleep(time.Millisecond * 10)
			continue
		} else if err != nil {
			return 0, err
		}

	}
}
