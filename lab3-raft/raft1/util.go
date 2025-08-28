package raft

import (
	"fmt"
	"os"
	"time"
)

const NO_TERM = -1

type who struct {
	state state
	id    int
	term  int
}

type verb int

const (
	SEND verb = iota
	RECV
	REPLY
)

var (
	debug = os.Getenv("DEBUG")
	start = time.Now()
)

func DPrintln1(from who, verb verb, to who, description string) {
	if debug == "" {
		return
	}
	switch debug {
	case "1", "CONCISE":
		fmt.Printf("%v %v %v: %s\n", from, verb, to, description)
	case "2", "VERBOSE":
		t := time.Since(start).Seconds()
		fmt.Printf("%.4fs %v %v %v: %s\n", t, from, verb, to, description)
	}
}

func DPrintln2(from who, description string) {
	if debug == "" {
		return
	}
	switch debug {
	case "1", "CONCISE":
		fmt.Printf("%v %v\n", from, description)
	case "2", "VERBOSE":
		t := time.Since(start).Seconds()
		fmt.Printf("%.4fs %v %v\n", t, from, description)
	}
}

func (v verb) String() string {
	switch v {
	case SEND:
		return "send"
	case RECV:
		return "recv"
	case REPLY:
		return "repl"
	}
	panic("unknown verb")
}

func (w who) String() string {
	var s string
	switch w.state {
	case follower:
		s = "follower"
	case candidate:
		s = "candidate"
	case leader:
		s = "leader"
	}
	if w.term == NO_TERM {
		return fmt.Sprintf("[%12s]", fmt.Sprintf("%v%v", s, w.id))
	}
	return fmt.Sprintf("[%12s]", fmt.Sprintf("%v%v-%v", s, w.id, w.term))
}
