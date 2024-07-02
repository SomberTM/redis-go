package main

import "fmt"

func ToSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func ToBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}