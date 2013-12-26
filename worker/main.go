package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"strconv"
)

var (
	connect = flag.String("c", "", "Address of the manager to connect to")
	port    = flag.Int("p", 1234, "The port on which to listen")
)

type Worker struct{}

func (Worker) Execute(cmd *string, _ *struct{}) error {
	log.Printf("executing [%s]\n", *cmd)

	c := exec.Command("/bin/sh", "-c", *cmd)
	o, err := c.CombinedOutput()
	if err != nil {
		log.Printf("failed output=[%s]: %s\n", o, err)
		return errors.New(err.Error() + ": [" + string(o) + "]")
	}

	log.Print("succeeded")
	return nil
}

func main() {
	flag.Parse()

	rpc.Register(new(Worker))
	l, e := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Print("listening for connections on ", l.Addr())

	if *connect != "" {
		log.Printf("calling %s\n", *connect)
		conn, err := net.Dial("tcp", *connect)
		if err != nil {
			log.Fatalf("failed to connect to %s: %s", *connect, err)
		}
		addr, _, err := net.SplitHostPort(conn.LocalAddr().String())
		if err != nil {
			log.Fatalf("failed to get the local address: %s", err)
		}
		laddr := net.JoinHostPort(addr, strconv.Itoa(*port))
		client := rpc.NewClient(conn)
		var res struct{}
		client.Call("WorkerList.Add", laddr, &res)
		client.Close()
	}

	rpc.Accept(l)
}
