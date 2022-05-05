package client

import (
	"bytes"
	"chukcha/protocol"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
)

const defaultScratchSize = 64 * 1024

// Client represents an instance of client connected to a set of Chukcha servers
type Simple struct {
	Logger *log.Logger

	addrs []string
	cl    *http.Client

	st *state
}

// a new client for chukcha server
func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
		st:    &state{Offsets: map[string]*ReadOffset{}},
	}
}

type ReadOffset struct {
	CurChunk          protocol.Chunk
	LastAckedChunkIdx int
	Off               uint64
}

type state struct {
	Offsets map[string]*ReadOffset
}

func (s *Simple) MarshalState() ([]byte, error) {
	return json.Marshal(s.st)
}

func (s *Simple) RestoreSavedState(buf []byte) error {
	return json.Unmarshal(buf, &s.st)
}

func (s *Simple) logger() *log.Logger {
	if s.Logger == nil {
		return log.Default()
	}

	return s.Logger
}

// Send sends the messages to the Chukcha servers.
func (s *Simple) Send(ctx context.Context, category string, msgs []byte) error {
	u := url.Values{}
	u.Add("category", category)

	url := s.getAddr() + "/write?" + u.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(msgs))
	if err != nil {
		return fmt.Errorf("making http request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.cl.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("sending data : http code %d %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

var errRetry = errors.New("please retry the request")

// Process will either wait for new messages or return an
// error in case sth goes wrong.
// The scratch buffer can be used to read
// The read offset will advance only if the process() function
// returns no errors for the data being processed
func (s *Simple) Process(ctx context.Context, category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addr := s.getAddr()

	if len(s.st.Offsets) == 0 {
		if err := s.updateCurrentChunks(ctx, category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}
	}

	for instance := range s.st.Offsets {
		err := s.processInstance(ctx, addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		}
		return err
	}

	return io.EOF
}

func (s *Simple) processInstance(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	for {
		if err := s.updateCurrentChunks(ctx, category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}

		err := s.process(ctx, addr, instance, category, scratch, processFn)
		if err == errRetry {
			continue
		}
		return err
	}
}

func (s *Simple) getAddr() string {
	addrIdx := rand.Intn(len(s.addrs))
	return s.addrs[addrIdx]
}

func (s *Simple) process(ctx context.Context, addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	curCh := s.st.Offsets[instance]

	u := url.Values{}
	u.Add("off", strconv.Itoa(int(curCh.Off)))
	u.Add("maxSize", strconv.Itoa(len(scratch)))
	u.Add("chunk", curCh.CurChunk.Name)
	u.Add("category", category)

	readURL := fmt.Sprintf("%s/read?%s", addr, u.Encode())

	resp, err := s.cl.Get(readURL)
	if err != nil {
		return fmt.Errorf("read %q: %v", readURL, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		io.Copy(io.Discard, resp.Body)
		s.logger().Printf("Chunk %+v is missing at %q, probably hasn't been replicated yet, skipping", curCh.CurChunk, addr)
		return nil
	} else if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d %s", resp.StatusCode, b.String())
	}

	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return fmt.Errorf("writing response: %v", err)
	}

	// 0 bytes read but no errors means the end of file by convention
	if b.Len() == 0 {
		if !curCh.CurChunk.Complete {
			if err := s.updateCurrentChunkCompleteStatus(ctx, curCh, instance, category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}

			if !curCh.CurChunk.Complete {
				if curCh.Off >= curCh.CurChunk.Size {
					return io.EOF
				}
			} else {
				return errRetry
			}
		}

		if curCh.Off < curCh.CurChunk.Size {
			return errRetry
		}

		if err := s.ackCurrentChunk(instance, category, addr); err != nil {
			return fmt.Errorf("ack current chunk: %v", err)
		}

		// need to read the next chunk so that we do not return empty response
		_, idx := protocol.ParseChunkFileName(curCh.CurChunk.Name)
		curCh.LastAckedChunkIdx = idx
		curCh.CurChunk = protocol.Chunk{}
		curCh.Off = 0

		return errRetry
	}

	err = processFn(b.Bytes())
	if err == nil {
		curCh.Off += uint64(b.Len())
	}

	return err
}

func (s *Simple) updateCurrentChunks(ctx context.Context, category, addr string) error {
	chunks, err := s.ListChunks(ctx, category, addr, false)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	if len(chunks) == 0 {
		return io.EOF
	}

	chunksByInstance := make(map[string][]protocol.Chunk)
	for _, c := range chunks {
		instance, chunkIdx := protocol.ParseChunkFileName(c.Name)
		if chunkIdx < 0 {
			continue
		}

		curChunk, exists := s.st.Offsets[instance]
		if exists && chunkIdx <= curChunk.LastAckedChunkIdx {
			continue
		}

		chunksByInstance[instance] = append(chunksByInstance[instance], c)
	}

	for instance, chunks := range chunksByInstance {
		curChunk, exists := s.st.Offsets[instance]
		if !exists {
			curChunk = &ReadOffset{}
		}

		if curChunk.CurChunk.Name == "" {
			curChunk.CurChunk = s.getOldestChunk(chunks)
			curChunk.Off = 0
		}

		s.st.Offsets[instance] = curChunk
	}

	return nil
}

func (s *Simple) getOldestChunk(chunks []protocol.Chunk) protocol.Chunk {
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Name < chunks[j].Name })

	for _, c := range chunks {
		if c.Complete {
			return c
		}
	}

	return chunks[0]
}

func (s *Simple) updateCurrentChunkCompleteStatus(ctx context.Context, curCh *ReadOffset, instance, category, addr string) error {
	chunks, err := s.ListChunks(ctx, category, addr, false)
	if err != nil {
		return fmt.Errorf("listChunks failed: %v", err)
	}

	// We need to prioritise the chunks that are complete
	// so that we ack them
	for _, c := range chunks {
		chunkInstance, idx := protocol.ParseChunkFileName(c.Name)
		if idx < 0 {
			continue
		}

		if chunkInstance != instance {
			continue
		}

		if c.Name == curCh.CurChunk.Name {
			curCh.CurChunk = c
			s.st.Offsets[instance] = curCh
			return nil
		}
	}

	return nil
}

func (s *Simple) ListChunks(ctx context.Context, category, addr string, fromReplication bool) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)
	if fromReplication {
		u.Add("from_replication", "1")
	}

	listRUL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", listRUL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}

	resp, err := s.cl.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("listChunks error: %s", string(body))
	}

	var res []protocol.Chunk
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Simple) ackCurrentChunk(instance, category, addr string) error {
	curCh := s.st.Offsets[instance]

	u := url.Values{}
	u.Add("chunk", curCh.CurChunk.Name)
	u.Add("size", strconv.Itoa(int(curCh.Off)))
	u.Add("category", category)

	resp, err := s.cl.Get(fmt.Sprintf(addr+"/ack?%s", u.Encode()))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, resp.Body)
		return fmt.Errorf("http code %d %s", resp.StatusCode, b.String())
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}
