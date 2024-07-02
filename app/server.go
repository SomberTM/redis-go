package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

type RequestContext struct {
	conn net.Conn
	raw []byte
}

func handleConnection(ctx RequestContext) {
	ctx.conn.Write([]byte("+PONG\r\n"))
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
		
	buf := make([]byte, 4096)
	var n int
	for {
		n, err = conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				conn.Close()	
				break;
			}

			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		if n == 0 {
			break
		}

		ctx := RequestContext {
			conn: conn,
			raw: buf,
		}
		go handleConnection(ctx)
	}

	
}
