package main

import (
	"flag"
	"net"
	"net/rpc"
	"strconv"
	"strings"
)

var port = flag.Int("p", 1235, "The port on which to listen for new workers")

func startWorkers(joblist *joblist) {
	rpc.Register(&WorkerList{joblist})
	l, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		logfile.Fatal("listen error:", err)
	}
	logfile.Print("listening for new workers on ", l.Addr())
	go rpc.Accept(l)

	for _, addr := range flag.Args() {
		go worker(addr, joblist)
	}
}

type WorkerList struct {
	joblist *joblist
}

func (w WorkerList) Add(addr *string, _ *struct{}) error {
	go worker(*addr, w.joblist)
	return nil
}

func worker(addr string, joblist *joblist) {
	logfile.Printf("worker %s: started\n", addr)

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		logfile.Printf("worker %s: %s", addr, err)
		return
	}
	logfile.Printf("worker %s: connected\n", addr)

	for j := range joblist.jobs {
		logfile.Printf("worker %s: got job [%s]\n", addr, j)

		var res struct{}
		err = client.Call("Worker.Execute", j, &res)
		if err != nil {
			if strings.HasPrefix(err.Error(), "exit status") {
				joblist.failJob(j, err.Error())
				continue
			}
			logfile.Printf("worker %s RPC error: %s\n", addr, err)
			joblist.repostJob(j)
			return
		}

		joblist.finishJob(j)
	}

	logfile.Printf("worker %s: done\n", addr)
}
