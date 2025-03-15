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
	cmd   *cobra.Command
	port  string
	store map[string]string
	mu    sync.RWMutex
}

func InitServer(port string, cmd *cobra.Command) *Server {
	return &Server{
		cmd:   cmd,
		port:  port,
		store: make(map[string]string),
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
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				s.cmd.ErrOrStderr().Write([]byte("could not read line, err: " + err.Error()))
			}
			break
		}

		response := s.processMessage(line, reader)
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

func (s *Server) processMessage(line string, reader *bufio.Reader) string {
	parts := strings.Fields(line)

	command, key := parts[0], parts[1]

	switch command {
	case "set":
		if len(parts) < 5 {
			return "CLIENT_ERROR invalid arguments\r\n"
		}

		byteCount, err := strconv.Atoi(parts[4])
		if err != nil {
			return "CLIENT_ERROR invalid byte count\r\n"
		}

		data := make([]byte, byteCount)
		n, err := reader.Read(data)

		if n != byteCount || err != nil {
			return "CLIENT_ERROR could not read data\r\n"
		}

		s.mu.Lock()
		s.store[key] = string(data)
		s.mu.Unlock()

		return "STORED\r\n"
	}

	return ""
}
