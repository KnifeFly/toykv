package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"toykv/config"
	"toykv/server"
)

var (
	listen   string
	raftDir  string
	raftBind string
	nodeID   string
	join     string
)

func init() {
	flag.StringVar(&listen, "server", ":7819", "toykb server listen address")
	flag.StringVar(&raftDir, "raftdir", "./", "raft storage directory")
	flag.StringVar(&raftBind, "raftbind", ":15379", "raft transport bind port")
	flag.StringVar(&nodeID, "id", "", "identification of node in toykv cluster")
	flag.StringVar(&join, "join", "", "join the exists toykv cluster")
}

func main() {
	flag.Parse()
	c := config.NewConfig(listen, raftDir, raftBind, nodeID, join)
	fmt.Println(c)

	toykv, err := server.NewServer(c)
	if err != nil {
		panic(err)
	}

	go toykv.Run()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sg := <-exit
	fmt.Fprintf(os.Stderr, "toykv exit, receive signal %s\n", sg.String())
}
