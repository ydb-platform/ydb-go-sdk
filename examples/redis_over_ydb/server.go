package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"
)

const (
	envTablePath = "YDB_KV_TABLE_PATH"
	envAPI       = "YDB_API"
)

// Server speaks a subset of the Redis RESP protocol backed by a [kvClientBuilder].
// Compatible with redis-cli and redis-benchmark for GET, SET, SETEX, DEL, KEYS, PING.
type (
	Server struct {
		net    string
		addr   string
		srv    *redcon.Server
		client interface {
		}
		log *slog.Logger
	}
	kvClient interface {
		Get(ctx context.Context, s string) ([]byte, error)
		Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error
		Del(ctx context.Context, keys ...string) (int, error)
		Keys(ctx context.Context, pattern string) ([]string, error)
	}
)

// NewServer creates a RESP server. It does not call [kvClientBuilder.Close]; stop the client when the database is closed.
func NewServer(network, addr string, client kvClient) *Server {
	log := slog.Default()
	h := func(conn redcon.Conn, cmd redcon.Command) {
		handleCommand(context.Background(), log, client, conn, cmd)
	}
	return &Server{
		net:    network,
		addr:   addr,
		client: client,
		log:    log,
		srv: redcon.NewServerNetwork(network, addr, h,
			func(conn redcon.Conn) bool {
				log.Debug("redis accept", "remote", conn.RemoteAddr())
				return true
			},
			func(conn redcon.Conn, err error) {
				if err != nil {
					log.Debug("redis close", "remote", conn.RemoteAddr(), "err", err)
				}
			},
		),
	}
}

// Start listens and serves until [Server.Stop] is called.
// When ready is non-nil, the server sends nil when listening or an error on failure.
func (s *Server) Start(ready chan error) error {
	s.log.Info("starting sugar redis server", "addr", s.addr)
	return s.srv.ListenServeAndSignal(ready)
}

// Stop closes the listener. The YDB [kvClientBuilder] is not closed.
func (s *Server) Stop() error {
	return s.srv.Close()
}

func handleCommand(ctx context.Context, log *slog.Logger, c kvClient, conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	name := strings.ToLower(string(cmd.Args[0]))
	args := cmd.Args[1:]
	switch name {
	case "ping":
		if len(args) > 1 {
			conn.WriteError("ERR wrong number of arguments for 'ping' command")
			return
		}
		if len(args) == 0 {
			conn.WriteString("PONG")
		} else {
			conn.WriteBulk(args[0])
		}
	case "echo":
		if len(args) != 1 {
			conn.WriteError("ERR wrong number of arguments for 'echo' command")
			return
		}
		conn.WriteBulk(args[0])
	case "select":
		// Single logical DB; accept for redis-cli compatibility.
		if len(args) != 1 {
			conn.WriteError("ERR wrong number of arguments for 'select' command")
			return
		}
		conn.WriteString("OK")
	case "quit", "close":
		conn.WriteString("OK")
		_ = conn.Close()
	case "hello":
		// RESP3 handshake (redis-cli 6+); RESP2 is enough for this server.
		if len(args) < 1 {
			conn.WriteError("ERR wrong number of arguments for 'hello' command")
			return
		}
		conn.WriteArray(6)
		conn.WriteBulkString("server")
		conn.WriteBulkString("sugar-redis")
		conn.WriteBulkString("version")
		conn.WriteBulkString("0.1")
		conn.WriteBulkString("proto")
		conn.WriteInt(2)
	case "command":
		// Minimal stub so newer redis-cli stays happy.
		conn.WriteArray(0)
	case "get":
		if len(args) != 1 {
			conn.WriteError("ERR wrong number of arguments for 'get' command")
			return
		}
		v, err := c.Get(ctx, string(args[0]))
		if err != nil {
			if errors.Is(err, io.EOF) {
				conn.WriteNull()
				return
			}
			log.Warn("GET", "err", err)
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteBulk(v)
	case "set":
		if len(args) < 2 {
			conn.WriteError("ERR wrong number of arguments for 'set' command")
			return
		}
		key := string(args[0])
		val := args[1]
		var d time.Duration
		for i := 2; i < len(args); i++ {
			flag := strings.ToLower(string(args[i]))
			switch flag {
			case "ex", "px":
				if i+1 >= len(args) {
					conn.WriteError("ERR syntax error")
					return
				}
				n, err := strconv.Atoi(string(args[i+1]))
				if err != nil || n < 0 {
					conn.WriteError("ERR value is not an integer or out of range")
					return
				}
				if flag == "ex" {
					d += time.Duration(n) * time.Second
				} else {
					d += time.Duration(n) * time.Millisecond
				}
				i++
			case "nx", "xx", "get", "keepttl":
				conn.WriteError("ERR unsupported SET option")
				return
			default:
				conn.WriteError("ERR syntax error")
				return
			}
		}
		if err := c.Set(ctx, key, val, &d); err != nil {
			log.Warn("SET", "err", err)
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case "setex":
		if len(args) != 3 {
			conn.WriteError("ERR wrong number of arguments for 'setex' command")
			return
		}
		key := string(args[0])
		sec, err := strconv.Atoi(string(args[1]))
		if err != nil || sec < 0 {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		ttl := time.Duration(sec) * time.Second
		if err := c.Set(ctx, key, args[2], &ttl); err != nil {
			log.Warn("SETEX", "err", err)
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteString("OK")
	case "del":
		if len(args) < 1 {
			conn.WriteError("ERR wrong number of arguments for 'del' command")
			return
		}
		keys := make([]string, len(args))
		for i := range args {
			keys[i] = string(args[i])
		}
		n, err := c.Del(ctx, keys...)
		if err != nil {
			log.Warn("DEL", "err", err)
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteInt(n)
	case "keys":
		if len(args) != 1 {
			conn.WriteError("ERR wrong number of arguments for 'keys' command")
			return
		}
		pat := string(args[0])
		keys, err := c.Keys(ctx, pat)
		if err != nil {
			log.Warn("KEYS", "err", err)
			conn.WriteError("ERR " + err.Error())
			return
		}
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulkString(k)
		}
	default:
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", name))
	}
}
