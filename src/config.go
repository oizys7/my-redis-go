package main

import (
	"flag"
	"my-redis-go/logging"
	"strconv"
	"strings"
	"sync"
)

var port = flag.String("port", "6379", "port to listen on")
var dir = flag.String("dir", "", "Directory to store RDB file")
var dbFileName = flag.String("dbfilename", "dump.rdb", "RDB file name")

// var logLevelStr = flag.String("loglevel", "INFO", "log print level")
var logLevel = flag.Int64("loglevel", 1, "log print level: 0 debug 1 info 2 warning 3 error 4 fatal 5 off")
var logger = logging.Logger{}

var Configs = map[string]string{}
var ConfigsMu = sync.RWMutex{}

func initConfigs() {
	// 解析命令行参数
	flag.Parse()

	logger = *logging.New(int(*logLevel))
	Configs["loglevel"] = strconv.FormatInt(*logLevel, 10)

	Configs["port"] = *port
	Configs["dir"] = *dir
	Configs["dbfilename"] = *dbFileName
}

func configGet(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: ERROR, str: "ERR wrong number of arguments for 'config get' command"}
	}
	cmd := args[0].bulk
	if strings.ToUpper(cmd) != "GET" {
		return Value{typ: ERROR, str: "ERR unknown command '" + cmd + "'"}
	}
	key := args[1].bulk
	ConfigsMu.RLock()
	value, ok := Configs[key]
	defer ConfigsMu.RUnlock()
	if !ok {
		return Value{typ: NULL}
	}
	var values []Value
	values = append(values, Value{typ: BULK, bulk: key})
	values = append(values, Value{typ: BULK, bulk: value})
	return Value{typ: ARRAY, array: values}
}
