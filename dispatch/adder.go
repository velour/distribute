package main

import (
	"github.com/velour/distribute"
	"net"
	"net/rpc"
	"strconv"
)

type JobAdder struct {
	joblist *joblist
}

// PushCommands pushes all the commands in the slice as individual entities
// This means that they may be interleaved with other job requests
func (ja JobAdder) PushCommands(commands *[]string, res *struct{}) error {
	for _, command := range *commands {
		if command == distribute.RemoteTerminationToken {
			// I don't know why but if someone stuck a termination token
			// in the middle of this command slice, don't post anything after it
			ja.joblist.eof <- true
			break
		} else {
			jobSlice := []string{command}
			ja.joblist.postJobs(jobSlice)
		}
	}
	return nil
}

// PushCommandsAtomic pushes all the commands in the slice as one entity
// This means that this won't be interleaved with other job requests
func (ja JobAdder) PushCommandsAtomic(commands *[]string, res *struct{}) error {

	posted := false
	for i, command := range *commands {
		// I don't know why but if someone stuck a termination token
		// in the middle of this command slice, don't post anything after it
		if command == distribute.RemoteTerminationToken {
			ja.joblist.postJobs((*commands)[0:i])
			ja.joblist.eof <- true
			posted = true
			break
		}
	}

	if !posted {
		ja.joblist.postJobs(*commands)
	}
	return nil
}

// StartAdders registers the jobadder and accepts
// incoming connections
func startAdders(joblist *joblist) {
	jobAdder := JobAdder{joblist}

	rpc.Register(&jobAdder)
	l, err := net.Listen("tcp", ":"+strconv.Itoa(*cmdport))
	if err != nil {
		logfile.Fatal("listen error:", err)
	}
	logfile.Print("listening for new commands on ", l.Addr())
	go rpc.Accept(l)
}
