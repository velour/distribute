package main

import (
	"bufio"
	"flag"
	"github.com/velour/distribute"
	"log"
	"net"
	"net/rpc"
	"os"
)

var (
	connect   = flag.String("c", "", "Address of the dispatcher to connect to")
	inpath    = flag.String("cmdfile", "cmds", "The command file")
	atomic    = flag.Bool("atomic", false, "Should this list of commands be run atomically")
	terminate = flag.Bool("terminate", false, "Ask the dispatcher to terminate when finished")
)

type JobAdder struct{}

func main() {
	flag.Parse()

	if *connect == "" {
		log.Fatalf("Dispatcher address not provided")
	}

	conn, err := net.Dial("tcp", *connect)
	if err != nil {
		log.Fatalf("failed to connect to %s: %s", *connect, err)
	}

	dispatcher := rpc.NewClient(conn)

	pushCommands(dispatcher)

	dispatcher.Close()
}

// PushCommands reads the command file
func pushCommands(dispatcher *rpc.Client) {

	infile, err := os.Open(*inpath)
	if err != nil {
		log.Fatalf("failed to open %s: %s\n", *inpath, err)
	}

	commands := make([]string, 0)

	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		commands = append(commands, scanner.Text())
	}

	if *terminate {
		commands = append(commands, distribute.RemoteTerminationToken)
	}

	var res struct{}
	if *atomic {
		err = dispatcher.Call("JobAdder.PushCommandsAtomic", &commands, &res)

	} else {
		dispatcher.Call("JobAdder.PushCommands", &commands, &res)
	}

	if err != nil {
		log.Fatalf(err.Error())
	}
}
