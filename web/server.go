package web

import (
	"chukcha/protocol"
	"chukcha/server"
	"chukcha/server/replication"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/valyala/fasthttp"
)

// Storage defines an interface for the backend storage
type Storage interface {
	Write(ctx context.Context, msgs []byte) error
	ListChunks() ([]protocol.Chunk, error)
	Read(chunk string, off uint64, maxSize uint64, w io.Writer) error
	Ack(chunk string, size uint64) error
}

// Server implements a web server
type Server struct {
	logger       *log.Logger
	instanceName string
	dirname      string
	listenAddr   string

	replClient  *replication.State
	replStorage *replication.Storage

	getOnDisk GetOnDiskFn
}

type GetOnDiskFn func(category string) (*server.OnDisk, error)

func NewServer(
	logger *log.Logger,
	replClient *replication.State,
	instanceName string,
	dirname string,
	listenAddr string,
	replStorage *replication.Storage,
	getOnDisk GetOnDiskFn,
) *Server {
	return &Server{
		logger:       logger,
		instanceName: instanceName,
		dirname:      dirname,
		listenAddr:   listenAddr,
		replClient:   replClient,
		replStorage:  replStorage,
		getOnDisk:    getOnDisk,
	}
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		s.writeHandler(ctx)
	case "/read":
		s.readHandler(ctx)
	case "/ack":
		s.ackHandler(ctx)
	case "/listChunks":
		s.listChunksHandler(ctx)
	default:
		ctx.WriteString("Hello world!")
	}
}

func isValidCatetory(category string) bool {
	if category == "" {
		return false
	}

	cleanPath := filepath.Clean(category)
	if cleanPath != category {
		return false
	}

	if strings.ContainsAny(category, `/\.`) {
		return false
	}

	return true
}

// get storage(ondisk) instance for category, if not found, create one
func (s *Server) getStorageForCategory(category string) (*server.OnDisk, error) {
	if !isValidCatetory(category) {
		return nil, errors.New("invalid category name")
	}

	return s.getOnDisk(category)
}

func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	if err := storage.Write(ctx, ctx.Request.Body()); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `chunk` GET param: chunk name must be provided"))
	}

	size, err := ctx.QueryArgs().GetUint("size")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `size` GET param: %v", err))
	}

	if err := storage.Ack(ctx, string(chunk), uint64(size)); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
	}
}

func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	chunk := ctx.QueryArgs().Peek("chunk")
	if len(chunk) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("bad `chunk` GET param: chunk name must be provided")
		return
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {

	}

	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `off` GET param: %v", err))
	}

	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(fmt.Sprintf("bad `maxSize` GET param: %v", err))
	}

	err = storage.Read(string(chunk), uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		if os.IsNotExist(err) {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		} else {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		}
		ctx.WriteString(err.Error())
		return
	}
}

func (s *Server) listChunksHandler(ctx *fasthttp.RequestCtx) {
	storage, err := s.getStorageForCategory(string(ctx.QueryArgs().Peek("category")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(err.Error())
		return
	}

	fromReplication, _ := ctx.QueryArgs().GetUint("from_replication")
	if fromReplication == 1 {

	}

	chunks, err := storage.ListChunks()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.WriteString(err.Error())
		return
	}

	json.NewEncoder(ctx).Encode(chunks)
}

func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(s.listenAddr, s.handler)
}
