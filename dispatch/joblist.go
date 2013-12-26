package main

import (
	"container/list"
	"log"
	"os"
	"strings"
)

var (
	faillog = logger("fail.log")
	oklog   = logger("ok.log")
)

const (
	resultOk = iota
	resultFail
	resultRepost
)

// result is the result of a job that a worker may pass back
// to the job list.
type result struct {
	status int
	cmd    string
	output string
}

// joblist stores the different jobs and distributes them to
// workers upon request.
type joblist struct {
	q                   *list.List
	n, nok, nfail, ntot int
	goteof              bool
	eof                 chan bool   // receiving EOF signal from command file
	post                chan string // receiving posted jobs
	jobs                chan string // sending jobs to workers
	done                chan result // receiving jobs completions
}

// newJoblist makes a new joblist
func newJoblist(finished chan<- bool) *joblist {
	j := &joblist{
		q:    list.New(),
		eof:  make(chan bool),
		post: make(chan string),
		jobs: make(chan string),
		done: make(chan result),
	}
	go j.Go(finished)
	return j
}

// postJob posts a new command to the joblist
func (j *joblist) postJob(cmd string) {
	j.ntot++
	j.post <- cmd
}

// failJob notifies the joblist that the given command failed
func (j *joblist) failJob(cmd string, output string) {
	j.done <- result{
		status: resultFail,
		cmd:    cmd,
		output: string(output),
	}
}

// finishJob notifies the joblist that the given command
// completed successfully
func (j *joblist) finishJob(cmd string) {
	j.done <- result{
		status: resultOk,
		cmd:    cmd,
	}
}

// repostJob notifies the joblist that the given command
// needs to be re-posted
func (j *joblist) repostJob(cmd string) {
	j.done <- result{
		status: resultRepost,
		cmd:    cmd,
	}
}

func (j *joblist) Go(finished chan<- bool) {

	barrierActive := false
	completionMark := 0

	for {
		var send chan<- string
		var front string
		if j.q.Len() > 0 {
			front = j.q.Front().Value.(string)

			if strings.Contains(front, "PLEASE WAIT HERE") {
				barrierActive = true
				j.n--
				completionMark = j.ntot - j.n

				logfile.Printf("Barrier Active\n")
				logfile.Printf("Barrier Tear Down In %d Job Completions\n",
					completionMark-(j.nok+j.nfail))

				e := j.q.Front()
				j.q.Remove(e)
			} else if !barrierActive {
				send = j.jobs
			}

		}
		select {
		case <-j.eof:
			logfile.Print("joblist: got EOF")
			j.goteof = true
			if j.n == 0 {
				goto done
			}

		case p := <-j.post:
			logfile.Printf("joblist: got post [%s]\n", p)
			j.n++
			j.q.PushBack(p)

		case p := <-j.done:
			if j.handleDone(p) {
				goto done
			}
			if barrierActive {
				logfile.Printf("Barrier Tear Down In %d Job Completions\n",
					completionMark-(j.nok+j.nfail))
				if (j.nok + j.nfail) == completionMark {
					logfile.Printf("Barrier Removed\n")
					barrierActive = false
				}
			}

		case send <- front:
			e := j.q.Front()
			logfile.Printf("joblist: sent [%s]\n", front)
			j.q.Remove(e)
		}
	}
done:
	finished <- true
}

// handleDone handles a result coming in on the done
// channel.  It returns true if all jobs are completed and
// there are no more jobs coming in from the command
// file.
func (j *joblist) handleDone(r result) bool {
	switch r.status {
	case resultOk:
		logfile.Printf("joblist: completed [%s]\n", r.cmd)
		oklog.Printf("[%s]\n", r.cmd)
		j.nok++
		j.n--

	case resultFail:
		logfile.Printf("joblist: failed [%s]\n", r.cmd)
		faillog.Printf("[%s] %s\n", r.cmd, r.output)
		j.nfail++
		j.n--

	case resultRepost:
		logfile.Printf("joblist: reposted [%s]\n", r.cmd)
		j.q.PushBack(r.cmd)
	}

	if j.goteof && j.n == 0 {
		logfile.Print("joblist: all done")
		close(j.jobs)
		return true
	}

	return false
}

// logger makes a new logger that logs to the given file
func logger(file string) *log.Logger {
	f, err := os.Create(file)
	if err != nil {
		logfile.Fatalf("failed to create log file %s: %s\n", file, err)
	}
	return log.New(f, "", log.LstdFlags)
}
