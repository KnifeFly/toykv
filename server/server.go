package server

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"toykv/config"
	"toykv/store"

	"github.com/siddontang/goredis"
)

var (
	// ErrParams param error
	ErrParams = errors.New("ERR params invalid")

	// ErrRespType response type invalid
	ErrRespType = errors.New("ERR resp type invalid")

	// ErrCmdNotSupport command not support
	ErrCmdNotSupport = errors.New("ERR command not supported")
)

// ToyKVServer toy key-value server
type ToyKVServer struct {
	listener net.Listener
	store    *store.Store
	logger   *log.Logger
}

// ToyKVClient toy key-value client
type ToyKVClient struct {
	// request is processing
	cmd  string
	args [][]byte

	buf     bytes.Buffer
	conn    net.Conn
	rReader *goredis.RespReader
	rWriter *goredis.RespWriter

	server *ToyKVServer
	logger *log.Logger
}

// NewServer return toykv server instance
func NewServer(c *config.Config) (*ToyKVServer, error) {
	tk := &ToyKVServer{
		logger: log.New(os.Stderr, "[toykv] ", log.LstdFlags|log.Lshortfile),
	}

	var err error
	tk.store = store.NewStore(c.RaftDir, c.RaftPort)

	bootstrap := (c.Join == "")
	err = tk.store.Init(bootstrap, c.NodeID)
	if err != nil {
		tk.logger.Print(err.Error())
		return nil, err
	}

	if !bootstrap {
		// send join request to node already exists
		rc := goredis.NewClient(c.Join, "")
		tk.logger.Printf("join request send to %s\n", c.Join)
		_, err := rc.Do("join", c.RaftPort, c.NodeID)
		if err != nil {
			tk.logger.Println(err)
		}
		rc.Close()
	}

	tk.listener, err = net.Listen("tcp", c.ListenAddress)
	if err != nil {
		tk.logger.Print(err.Error())
		return nil, err
	}

	tk.logger.Printf("toykv listen %s successfully", c.ListenAddress)
	return tk, nil
}

// Run toykv server main loop
func (t *ToyKVServer) Run() {
	for {
		con, err := t.listener.Accept()
		if err != nil {
			t.logger.Printf("accept error %v", err)
			continue
		}

		// new connection handler
		t.connectHandler(con)
	}
}

// connectHandler handle new client connection
func (t *ToyKVServer) connectHandler(conn net.Conn) {
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	client := &ToyKVClient{
		conn:    conn,
		server:  t,
		logger:  t.logger,
		rReader: goredis.NewRespReader(br),
		rWriter: goredis.NewRespWriter(bw),
	}

	go client.clientHandler()
}

// clientHandler handle client request
func (c *ToyKVClient) clientHandler() {
	defer func(c *ToyKVClient) {
		c.conn.Close()
	}(c)

	for {
		c.cmd = ""
		c.args = nil

		req, err := c.rReader.ParseRequest()
		if err != nil && err != io.EOF {
			c.logger.Println(err.Error())
			continue
		} else if err != nil {
			return
		}

		err = c.requestHandler(req)
		if err != nil {
			c.logger.Println(err.Error())
			return
		}
	}
}

func (c *ToyKVClient) requestHandler(req [][]byte) error {
	if len(req) == 0 {
		c.cmd = ""
		c.args = nil
	} else {
		c.cmd = strings.ToLower(string(req[0]))
		c.args = req[1:]
	}

	c.logger.Printf("process %s command", c.cmd)

	var (
		v   string
		err error
	)

	switch c.cmd {
	case "ping":
		if err = c.handlePing(); err == nil {
			c.Flush("PONG")
		}
	case "set":
		if err = c.handleSet(); err == nil {
			c.Flush("OK")
		}
	case "join":
		if err = c.handleJoin(); err == nil {
			c.Flush("OK")
		}
	case "leave":
		if err = c.handleLeave(); err == nil {
			c.Flush("OK")
		}
	case "del":
		if err = c.handleDel(); err == nil {
			c.Flush("OK")
		}
	case "get":
		if v, err = c.handleGet(); err == nil {
			c.Flush(v)
		}
	default:
		err = ErrCmdNotSupport
	}

	if err != nil {
		c.logger.Print(err.Error())
		c.Flush(err)
	}
	return nil
}

// Write write response to client
func (c *ToyKVClient) Write(resp interface{}) error {
	var err error = nil

	switch v := resp.(type) {
	case []interface{}:
		err = c.rWriter.WriteArray(v)
	case []byte:
		err = c.rWriter.WriteBulk(v)
	case nil:
		err = c.rWriter.WriteBulk(nil)
	case int64:
		err = c.rWriter.WriteInteger(v)
	case string:
		err = c.rWriter.WriteString(v)
	case error:
		err = c.rWriter.WriteError(v)
	default:
		err = ErrRespType
	}
	return err
}

// Flush flush client response
func (c *ToyKVClient) Flush(data interface{}) error {
	err := c.Write(data)
	if err != nil {
		return err
	}
	return c.rWriter.Flush()
}
