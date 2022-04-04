package main

import (
	"chukcha/client"
	"errors"
	"fmt"
	"go/build"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	maxN          = 10000000
	maxBufferSize = 1024 * 1024

	sendFmt = "Send: net %13s, cpu %13s (%.1f MiB)"
	recvFmt = "Recv: net %13s, cpu %13s"
)

func main() {
	if err := runTest(); err != nil {
		log.Fatalf("Test failed: %v", err)
	}
	log.Printf("The test passed!")
}

func runTest() error {
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = build.Default.GOPATH
	}

	log.Printf("Compiling chukcha")
	out, err := exec.Command("go", "install", "-v", "chukcha").CombinedOutput()
	if err != nil {
		log.Printf("Failed to build :%v", err)
		return fmt.Errorf("compilation failed:L %v (out: %s)", err, string(out))
	}

	port := 8061

	dbPath := "/tmp/chukcha.db"
	os.Remove(dbPath)

	log.Printf("Running chukcha on port %d", port)

	cmd := exec.Command(goPath+"/bin/chukcha", "-inmem", "-filename="+dbPath, fmt.Sprintf("-port=%d", port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	defer cmd.Process.Kill()

	log.Printf("Waiting for the port localhost:%d to open", port)
	for i := 0; i <= 100; i++ {
		timeout := time.Millisecond * 50
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(port)), timeout)
		if err != nil {
			time.Sleep(timeout)
			continue
		}
		conn.Close()
		break
	}

	log.Printf("Starting the test")

	s := client.NewSimple([]string{fmt.Sprintf("http://localhost:%d", port)})
	want, err := send(s)
	if err != nil {
		return fmt.Errorf("send error: %v", err)
	}

	got, err := receive(s)
	if err != nil {
		return fmt.Errorf("receive error: %v", err)
	}

	if want != got {
		return fmt.Errorf("the expected sum %d is not equal to the actual sum %d", want, got)
	}

	return nil
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
			if err := s.Send(buf); err != nil {
				return 0, err
			}
			networkTime += time.Since(start)
			sendBytes += len(buf)

			buf = buf[0:0]
		}
	}

	if len(buf) != 0 {
		start := time.Now()
		if err := s.Send(buf); err != nil {
			return 0, err
		}
		networkTime += time.Since(start)
		sendBytes += len(buf)
	}

	return sum, nil
}

func receive(s *client.Simple) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)

	var parseTime time.Duration
	receiveStart := time.Now()
	defer func() {
		log.Printf(recvFmt, time.Since(receiveStart)-parseTime, parseTime)
	}()

	trimNL := func(r rune) bool {
		return r == '\n'
	}

	for {
		res, err := s.Receive(buf)
		if errors.Is(err, io.EOF) {
			return sum, nil
		} else if err != nil {
			return 0, err
		}

		start := time.Now()

		ints := strings.Split(strings.TrimRightFunc(string(res), trimNL), "\n")
		for _, str := range ints {
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}
			sum += int64(i)
		}

		parseTime += time.Since(start)
	}
}
