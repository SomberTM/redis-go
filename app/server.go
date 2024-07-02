package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

var kv map[string] string = make(map[string]string)

type RedisServer struct {
	role string
	masterReplid string
	replOffset int
	replicaAddresses []net.Addr
	connections []net.Conn
	masterConnection net.Conn
	storedWriteCommands [][]byte
}

func (ctx *RedisServer) AddReplica(conn net.Conn) {
	ctx.replicaAddresses = append(ctx.replicaAddresses, conn.RemoteAddr())
}

func (server *RedisServer) GetReplicaConnections() []net.Conn {
	connections := make([]net.Conn, 0, len(server.connections))
	for i := 0; i < len(server.connections); i++ {
		if slices.Contains(server.replicaAddresses, server.connections[i].RemoteAddr()) {
			connections = append(connections, server.connections[i])
		}
	}
	return connections
}

func (server *RedisServer) PropagateToReplicas(b []byte) {
	if server.role != "master" {
		return
	}

	replicas := server.GetReplicaConnections()
	for i := 0; i < len(replicas); i++ {
		replica := replicas[i]
		_, err := replica.Write(b)
		if err != nil {
			fmt.Println("Error writing to replica at", replica.RemoteAddr())
		}
		fmt.Println("Wrote to replica at", replica.RemoteAddr())
	}
}

func (server *RedisServer) AddStoredCommand(c []byte) {
	server.storedWriteCommands = append(server.storedWriteCommands, c)
}

func (server *RedisServer) WriteStoredCommands(conn net.Conn) {
	fmt.Println("Propagating stored commands", server.storedWriteCommands, "to replica at", conn.RemoteAddr())
	for i := 0; i < len(server.storedWriteCommands); i++ {
		_, err := conn.Write(server.storedWriteCommands[i])
		if err != nil {
			fmt.Println("Error writing stored command", server.storedWriteCommands[i], "to replica at", conn.RemoteAddr())
		}
	}
}


type RequestContext struct {
	conn net.Conn
	raw []byte
}

func ToRaw(s string) string {
	var buf bytes.Buffer
	for i := 0; i < len(s); i++ {
		switch s[i] {
			case '\r':
				buf.WriteString("\\r")
			case '\n':
				buf.WriteString("\\n")
			default:
				buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func handleConnection(conn net.Conn) {
	ctx := RequestContext {
			conn: conn,
			raw: nil,
	}

	defer conn.Close()

	for {
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break;
			}

			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		if n == 0 {
			break
		} 

		// why I have to do this, cry
		ctx.raw = []byte(strings.ReplaceAll(string(buf), "\x00", ""))
		parser := NewRespParser(buf)

		for !parser.Done() {
			data, err := parser.Next()
			if err != nil {
				if err == io.EOF {
					break
				}

				fmt.Println("Error parsing resp request: ", err.Error())
				continue
			}

			if data.data_type == Array {
				switch value := data.value.(type) {
					case [][]byte:
						strs := make([]string, 0, len(value))
						for i := 0; i < len(value); i++ {
							strs = append(strs, string(value[i]))
						}
						handleCommand(ctx, strs)
				}
			}
		}
		
	}
}

func deleteKeyAfter(key string, ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	delete(kv, key)
}

func isReplicatedCommand(c string) bool {
	return c == "SET"
}

func handleCommand(ctx RequestContext, s []string) {
	command, args := strings.ToUpper(s[0]), s[1:]
	fmt.Println("Handling command", command, "with args", args)

	var response []byte

	switch command {
		case "PING":
			response = ToSimpleString("PONG")
		case "ECHO":
			response = ToBulkString(args[0])
		case "SET":
			kv[args[0]] = args[1]
			if len(args) > 3 && strings.ToUpper(args[2]) == "PX" {
				ms, err := strconv.Atoi(args[3])

				if err != nil {
					fmt.Println("Error parsing expiry: ", err.Error())
					ctx.conn.Write(ToSimpleError("Bad request"))
					return
				}

				go deleteKeyAfter(args[0], ms)
			}
			response = []byte(OkSimpleString)
		case "GET":
			v, ok := kv[args[0]]
			if ok {
				response = ToBulkString(v)
			} else {
				ctx.conn.Write([]byte(NilBulkString))
			}
		case "INFO":
			if len(args) == 0 {
				response = ToSimpleError("Invalid INFO usage")
				break
			}

			switch strings.ToUpper(args[0]) {
				case "REPLICATION":
					response = ToBulkString(fmt.Sprintf("# Replication\nrole:%s\nconnected_slaves:0\nmaster_replid:%s\nmaster_repl_offset:0\n", server.role, server.masterReplid))
				default:
					response = ToSimpleError("Unsupported INFO argument")
			}
		case "REPLCONF":
			conf := strings.ToUpper(args[0])
			switch conf {
				case "LISTENING-PORT", "CAPA":
					response = []byte(OkSimpleString)
				case "GETACK":
					ctx.conn.Write(ToRespArray([]string{ "REPLCONF", "ACK", "0" }))
				case "ACK":
					break
				default:
					response = ToSimpleError("Invalid configuration option")
			}
		case "PSYNC":
			ctx.conn.Write(ToSimpleString(fmt.Sprintf("FULLRESYNC %s 0", server.masterReplid)))
			rdb, err := os.ReadFile("data/empty.rdb")
			if err != nil {
				fmt.Println("Error reading empty rdb", err.Error())
				os.Exit(1)
			}
			var buf bytes.Buffer
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(rdb)))
			buf.Write(rdb)
			ctx.conn.Write(buf.Bytes())

			server.AddReplica(ctx.conn)
			server.WriteStoredCommands(ctx.conn)

			ctx.conn.Write(ToRespArray([]string{ "REPLCONF", "GETACK", "*" }))
		default:
			ctx.conn.Write(ToSimpleError("Unsupported command"))
	}

	if response != nil || (server.role == "slave" && server.masterConnection != ctx.conn) {
		ctx.conn.Write(response)
	}

	if server.role == "master" && isReplicatedCommand(command) {
		server.AddStoredCommand(ctx.raw)
		server.PropagateToReplicas(ctx.raw)	
	}
}

func pargsToMap() map[string] string {
	args := os.Args[1:]
	argmap := make(map[string] string)

	if len(args) == 0 {
		return argmap
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			arg_value := args[i + 1]
			argmap[arg[2:]] = arg_value
			i++
		}
	}

	return argmap
}

var server RedisServer = RedisServer {
	role: "master",
	masterReplid: "hellomom",
	replOffset: 0,
	replicaAddresses: make([]net.Addr, 0, 10),
	connections: make([]net.Conn, 0, 10),
	masterConnection: nil,
	storedWriteCommands: make([][]byte, 0),
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	args := pargsToMap()

	port, portok := args["port"]
	if !portok {
		port = "6379"
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	fmt.Println("Listening on port", port)

	master_info, replicaok := args["replicaof"]
	if replicaok {
		server.role = "slave"
		mhost, mport, found := strings.Cut(master_info, " ")
		if found {
			address := fmt.Sprintf("%s:%s", mhost, mport)
			mconn, rerr := net.Dial("tcp", address)
			if rerr != nil {
				fmt.Println("Failed to connect to master at", address)
			}
			fmt.Println("Connected to master at", address)
			server.masterConnection = mconn

			buf := make([]byte, 4096)

			mconn.Write(ToRespArray([]string{ "PING" }))
			mconn.Read(buf)
			
			mconn.Write(ToRespArray([]string{ "REPLCONF", "listening-port", port }))
			mconn.Read(buf)

			mconn.Write(ToRespArray([]string{ "REPLCONF", "capa", "psync2" }))
			mconn.Read(buf)

			mconn.Write(ToRespArray([]string{ "PSYNC", "?", "-1" }))
			mconn.Read(buf)
			mconn.Read(buf)

			go handleConnection(mconn)
		}
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
			
		server.connections = append(server.connections, conn)
		go handleConnection(conn)
	}
}
