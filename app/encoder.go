package main

import "fmt"

const OkSimpleString = "+OK\r\n" 
const NilBulkString = "$-1\r\n"

func ToSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func ToBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}