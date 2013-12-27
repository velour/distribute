package main

import (
	"bufio"
	"flag"
	"log"
	"os"
)

var (
	logfile = log.New(os.Stderr, "", log.LstdFlags)
	inpath  = flag.String("cmdfile", "", "The command file")
	cmdport  = flag.Int("cmdport", 2222, "The port on which to listen for workers")
)

func main() {
	flag.Parse()
	finished := make(chan bool)
	joblist := newJoblist(finished)

	useCmdFile := false
	useCmdPort := false
	markFlags := func(f *flag.Flag) {
		if f.Name == "cmdport" {
			useCmdPort = true
		} else if f.Name == "cmdfile" {
			useCmdFile = true
		}
	}
	flag.Visit(markFlags)

	startWorkers(joblist)

	if useCmdFile {
		postCommandsFromFile(joblist, useCmdPort)
	}
	if !useCmdFile || useCmdPort {
		go startAdders(joblist)
	}

	<-finished
	logfile.Printf("%d jobs succeeded\n", joblist.nok)
	logfile.Printf("%d jobs failed\n", joblist.nfail)
	logfile.Printf("%d jobs completed\n", joblist.nok+joblist.nfail)
}

// PostCommands reads the command file and posts
// each line as a command to the joblist.
func postCommandsFromFile(joblist *joblist, useCmdPort bool) {
	infile, err := os.Open(*inpath)
	if err != nil {
		logfile.Fatalf("failed to open %s: %s\n", *inpath, err)
	}

	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		jobSlice := []string{scanner.Text()}
		joblist.postJobs(jobSlice)
	}

	// We don't know when we'll get more commands if we're listening
	// on a port, so don't terminate the joblist when we read to the
	// end of the file
	if !useCmdPort {
		joblist.eof <- true
	}
}