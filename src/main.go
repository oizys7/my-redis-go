package main

import "my-redis-go/logging"

var logger = logging.New(logging.LevelDebug)

func main() {
	initConfigs()
	loadRdbFileIntoKVMemoryStore()

	server := &Server{}
	defer server.Close()
	server.Start()
}
