package internal

import (
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
	// TODO: implement
}
