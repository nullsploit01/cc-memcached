package internal

import (
	"bufio"
	"net"
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
			s.cmd.ErrOrStderr().Write([]byte("could not read line, err: " + err.Error()))
			break
		}

		_, err = c.Write([]byte(line))
		if err != nil {
			s.cmd.ErrOrStderr().Write([]byte("could not write line, err: " + err.Error()))
			break
		}
	}
}
