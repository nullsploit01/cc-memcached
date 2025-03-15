package internal

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

type Server struct {
	cmd         *cobra.Command
	port        string
	store       map[string]string
	mu          sync.RWMutex
	pendingData map[net.Conn]*pendingSet
}

type pendingSet struct {
	key       string
	byteCount int
}

func InitServer(port string, cmd *cobra.Command) *Server {
	return &Server{
		cmd:         cmd,
		port:        port,
		store:       make(map[string]string),
		pendingData: make(map[net.Conn]*pendingSet),
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
		if pending, exists := s.pendingData[c]; exists {
			data := make([]byte, pending.byteCount)
			_, err := io.ReadFull(reader, data)
			if err != nil {
				c.Write([]byte("CLIENT_ERROR invalid byte count\r\n"))
				delete(s.pendingData, c)
				continue
			}

			ending, err := reader.ReadString('\n')
			if err != nil || !strings.HasSuffix(ending, "\r\n") {
				c.Write([]byte("CLIENT_ERROR bad data block termination\r\n"))
				delete(s.pendingData, c)
				continue
			}

			s.mu.Lock()
			s.store[pending.key] = string(data)
			s.mu.Unlock()

			c.Write([]byte("STORED\r\n"))
			delete(s.pendingData, c)
			continue
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				s.cmd.ErrOrStderr().Write([]byte("could not read line, err: " + err.Error()))
			}
			break
		}

		response, expectingData, key, byteCount := s.processMessage(strings.TrimSpace(line))

		if expectingData {
			s.pendingData[c] = &pendingSet{
				key:       key,
				byteCount: byteCount,
			}

			continue
		}

		if response != "" {
			_, err = c.Write([]byte(response))
			if err != nil {
				s.cmd.ErrOrStderr().Write([]byte("could not write response, err: " + err.Error()))
				break
			}
			continue
		}
	}
}

func (s *Server) processMessage(line string) (response string, expectingData bool, key string, byteCount int) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "ERROR\r\n", false, "", 0
	}

	command, key := parts[0], parts[1]

	switch command {
	case "set":
		if len(parts) < 5 {
			return "CLIENT_ERROR invalid arguments\r\n", false, "", 0
		}

		byteCount, err := strconv.Atoi(parts[4])
		if err != nil {
			return "CLIENT_ERROR invalid byte count\r\n", false, "", 0
		}

		return "", true, key, byteCount

	default:
		return "ERROR\r\n", false, "", 0
	}
}
