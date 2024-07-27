package logx

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

var (
	debugStart time.Time
	debug      int
)

type Topic string

const (
	DClient  Topic = "CLNT"
	DCommit  Topic = "CMIT"
	DDrop    Topic = "DROP"
	DLeader  Topic = "LEAD"
	DLog1    Topic = "LOG1"
	DLog2    Topic = "LOG2"
	DPersist Topic = "PERS"
	DSnap    Topic = "SNAP"
	DTerm    Topic = "TERM"
	DTimer   Topic = "TIMR"
	DVote    Topic = "VOTE"
	
	DInfo  Topic = "INFO"
	DWarn  Topic = "WARN"
	DError Topic = "ERRO"
)

func init() {
	debug = level()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic Topic, format string, a ...interface{}) {
	if debug == 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v", time, topic)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func level() int {
	v := os.Getenv("DEBUG")
	lv := 0
	if v != "" {
		var err error
		lv, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid DEBUG %v", v)
		}
	}
	return lv
}
