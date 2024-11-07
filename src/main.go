package main

func main() {
	initConfigs()
	loadRdbFileIntoKVMemoryStore()

	server := &Server{}
	defer server.Close()
	server.Start()
}
