package raft

import (
	"encoding/json"
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Marshal(v interface{}) string {
	res, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	return string(res)
}
