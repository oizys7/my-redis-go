package main

func main() {
	initConfigs()

	server := &Server{}
	defer server.Close()
	server.Start()
}
