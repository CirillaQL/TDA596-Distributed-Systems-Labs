package main

import (
	"log"
	"os"
	"strconv"

	"github.com/CirillaQL/TDA596-Distributed-Systems-Labs/lab1/server"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <port (1-65535)>", os.Args[0])
	}
	port, err := strconv.ParseUint(os.Args[1], 10, 16)
	if err != nil || port == 0 {
		log.Fatal("Port must be an integer between 1 and 65535")
	}
	if err := server.NewServer(uint16(port)).Start(); err != nil {
		log.Fatal(err)
	}
}
