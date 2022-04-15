package client

import (
	"bytes"
	"chukcha/protocol"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
)

const defaultScratchSize = 64 * 1024

// Client represents an instance of client connected to a set of Chukcha servers
type Simple struct {
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

// Send sends the messages to the Chukcha servers.
func (s *Simple) Send(category string, msgs []byte) error {
	u := url.Values{}
	u.Add("category", category)

	url := s.getAddr() + "/write?" + u.Encode()
	resp, err := s.cl.Post(url, "application/octet-stream", bytes.NewReader(msgs))
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
func (s *Simple) Process(category string, scratch []byte, processFn func([]byte) error) error {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addr := s.getAddr()

	if len(s.st.Offsets) == 0 {
		if err := s.updateCurrentChunks(category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}
	}

	for instance := range s.st.Offsets {
		err := s.processInstance(addr, instance, category, scratch, processFn)
		if errors.Is(err, io.EOF) {
			continue
		}
		return err
	}

	return io.EOF
}

func (s *Simple) processInstance(addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
	for {
		if err := s.updateCurrentChunks(category, addr); err != nil {
			return fmt.Errorf("updateCurrentChunk: %w", err)
		}

		err := s.process(addr, instance, category, scratch, processFn)
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

func (s *Simple) process(addr, instance, category string, scratch []byte, processFn func([]byte) error) error {
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

	if resp.StatusCode != http.StatusOK {
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
			if err := s.updateCurrentChunkCompleteStatus(curCh, instance, category, addr); err != nil {
				return fmt.Errorf("updateCurrentChunkCompleteStatus: %v", err)
			}

			if !curCh.CurChunk.Complete {
				if curCh.Off >= curCh.CurChunk.Size {
					return io.EOF
				}

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

func (s *Simple) updateCurrentChunks(category, addr string) error {
	chunks, err := s.ListChunks(category, addr)
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

func (s *Simple) updateCurrentChunkCompleteStatus(curCh *ReadOffset, instance, category, addr string) error {
	chunks, err := s.ListChunks(category, addr)
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

func (s *Simple) ListChunks(category, addr string) ([]protocol.Chunk, error) {
	u := url.Values{}
	u.Add("category", category)

	listRUL := fmt.Sprintf("%s/listChunks?%s", addr, u.Encode())

	resp, err := s.cl.Get(listRUL)
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
