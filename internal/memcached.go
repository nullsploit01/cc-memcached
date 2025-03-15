package internal

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

type Server struct {
	cmd         *cobra.Command
	port        string
	store       map[string]entry
	mu          sync.RWMutex
	pendingData map[net.Conn]*pendingSet
	pendingCmd  map[net.Conn]string
}

type entry struct {
	value      string
	expiration int64 // Unix timestamp when the key expires (0 means no expiry)
}

type pendingSet struct {
	key       string
	byteCount int
	expTime   int64
}

func InitServer(port string, cmd *cobra.Command) *Server {
	return &Server{
		cmd:         cmd,
		port:        port,
		mu:          sync.RWMutex{},
		store:       make(map[string]entry),
		pendingData: make(map[net.Conn]*pendingSet),
		pendingCmd:  make(map[net.Conn]string),
	}
}

func (s *Server) StartServer() error {
	l, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return err
	}

	defer l.Close()

	s.cmd.OutOrStdout().Write([]byte("Listening on " + l.Addr().String() + "\n"))

	for {
		c, err := l.Accept()
		if err != nil {
			s.cmd.ErrOrStderr().Write([]byte("could not accept connection, err: " + err.Error()))
			continue
		}
		go s.handleConnection(c)
	}
}

func (s *Server) handleConnection(c net.Conn) {
	defer c.Close()
	s.cmd.OutOrStdout().Write([]byte("Accepted connection from " + c.RemoteAddr().String() + "\n"))

	reader := bufio.NewReader(c)
	for {
		s.mu.RLock()
		pending, exists := s.pendingData[c]
		s.mu.RUnlock()
		if exists {
			data := make([]byte, pending.byteCount)
			_, err := io.ReadFull(reader, data)
			if err != nil {
				c.Write([]byte("CLIENT_ERROR invalid byte count\r\n"))
				s.mu.Lock()
				delete(s.pendingData, c)
				delete(s.pendingCmd, c)
				s.mu.Unlock()
				continue
			}

			ending, err := reader.ReadString('\n')
			if err != nil || !strings.HasSuffix(ending, "\r\n") {
				c.Write([]byte("CLIENT_ERROR bad data block termination\r\n"))
				s.mu.Lock()
				delete(s.pendingData, c)
				delete(s.pendingCmd, c)
				s.mu.Unlock()
				continue
			}

			s.mu.RLock()
			cmd := s.pendingCmd[c]
			s.mu.RUnlock()
			expiration := pending.expTime

			if expiration > 0 && expiration < 2592000 { // 30 days
				expiration += time.Now().Unix()
			}
			s.mu.Lock()
			switch cmd {
			case "set":
				s.store[pending.key] = entry{
					value:      string(data),
					expiration: expiration,
				}
				c.Write([]byte("STORED\r\n"))

			case "add":
				if _, e := s.store[pending.key]; e {
					c.Write([]byte("NOT_STORED\r\n"))
				} else {
					s.store[pending.key] = entry{
						value:      string(data),
						expiration: expiration,
					}
					c.Write([]byte("STORED\r\n"))
				}

			case "replace":
				if _, e := s.store[pending.key]; e {
					s.store[pending.key] = entry{
						value:      string(data),
						expiration: expiration,
					}
					c.Write([]byte("STORED\r\n"))
				} else {
					c.Write([]byte("NOT_STORED\r\n"))
				}

			case "append":
				if existingEntry, e := s.store[pending.key]; e {
					s.store[pending.key] = entry{
						value:      existingEntry.value + string(data),
						expiration: existingEntry.expiration,
					}
					c.Write([]byte("STORED\r\n"))
				} else {
					c.Write([]byte("NOT_STORED\r\n"))
				}

			case "prepend":
				if existingEntry, e := s.store[pending.key]; e {
					s.store[pending.key] = entry{
						value:      string(data) + existingEntry.value,
						expiration: existingEntry.expiration,
					}
					c.Write([]byte("STORED\r\n"))
				} else {
					c.Write([]byte("NOT_STORED\r\n"))
				}
			}

			delete(s.pendingData, c)
			delete(s.pendingCmd, c)
			s.mu.Unlock()
			continue
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				s.cmd.ErrOrStderr().Write([]byte("could not read line, err: " + err.Error()))
			}
			break
		}

		response, expectingData, key, byteCount, expTime, pendingCmd := s.processMessage(strings.TrimSpace(line))

		if expectingData {
			s.mu.Lock()
			s.pendingData[c] = &pendingSet{
				key:       key,
				byteCount: byteCount,
				expTime:   expTime,
			}
			s.pendingCmd[c] = pendingCmd
			s.mu.Unlock()
			continue
		}

		if response != "" {
			_, err = c.Write([]byte(response))
			if err != nil {
				s.cmd.ErrOrStderr().Write([]byte("could not write response, err: " + err.Error()))
				break
			}
		}
	}
}

func (s *Server) processMessage(line string) (response string, expectingData bool, key string, byteCount int, expTime int64, pendingCmd string) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "ERROR\r\n", false, "", 0, 0, ""
	}

	command, key := parts[0], parts[1]

	switch command {
	case "set", "add", "replace", "append", "prepend":
		if len(parts) < 5 {
			return "CLIENT_ERROR invalid arguments\r\n", false, "", 0, 0, ""
		}

		exptimeInt, err := strconv.Atoi(parts[3])
		if err != nil || exptimeInt < 0 {
			return "CLIENT_ERROR invalid exptime\r\n", false, "", 0, 0, ""
		}

		byteCount, err := strconv.Atoi(parts[4])
		if err != nil {
			return "CLIENT_ERROR invalid byte count\r\n", false, "", 0, 0, ""
		}

		return "", true, key, byteCount, int64(exptimeInt), command

	case "get":
		s.mu.RLock()
		e, exists := s.store[key]
		s.mu.RUnlock()

		if exists && (e.expiration == 0 || e.expiration > time.Now().Unix()) {
			return fmt.Sprintf("VALUE %s 0 %d\r\n%s\r\nEND\r\n", key, len(e.value), e.value), false, "", 0, 0, ""
		}

		if exists {
			s.mu.Lock()
			delete(s.store, key)
			s.mu.Unlock()
		}

		return "END\r\n", false, "", 0, 0, ""

	default:
		return "ERROR\r\n", false, "", 0, 0, ""
	}
}
