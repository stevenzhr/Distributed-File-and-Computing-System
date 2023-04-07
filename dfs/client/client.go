package main

import (
	"bufio"
	. "dfs/client/modules"
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

// var (
// 	msgHandler *utility.MessageHandler
// 	wg         sync.WaitGroup
// )

//go run client.go orion01:28998
//1 - controller listen port
func main() {
	// init log output
	logFile, err := utility.SetLogFile("client", os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// start connecting controller
	msgHandler, err := createConnection(os.Args[1])
	defer msgHandler.Close()
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	SetMsgHandler(msgHandler)
	fmt.Println("Successfully connect to DFS controller, please input your command: ")

	alive := true
	// Read instruction
	for alive {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("-> ")
		scanner.Scan()
		reqStrs := strings.Split(scanner.Text(), " ")
		if len(reqStrs) < 1 {
			fmt.Println("Wrong command, e.g. store test.txt")
		}
		// read first part of command and switch it:
		switch reqStrs[0] {
		case "exit":
			fmt.Println("Thank you for using Wonderpea Distributed File System. Bye~")
			return
		case "store": // store filename filesize(MB)
			if len(reqStrs) < 2 {
				fmt.Println("Please input upload filename. [store example.txt 5]")
			} else {
				alive = StoreFile(reqStrs)
			}
		case "get": //get filename
			if len(reqStrs) != 2 {
				fmt.Println("Please input get filename. [get example.txt]")
			} else {
				alive = GetFile(reqStrs)
			}
		case "delete": // delete filename
			if len(reqStrs) != 2 {
				fmt.Println("Please input upload filename. [delete example.txt]")
			} else {
				alive = DeleteFile(reqStrs)
			}
		case "ls": // list file
			if len(reqStrs) > 3 {
				fmt.Println("Please input correct ls command. [ls example.txt(optional)]")
			} else {
				alive = ListFile(reqStrs)
			}
		case "lsn":
			if len(reqStrs) > 2 {
				fmt.Println("Please input correct lsn command. [lsn nodeName(optional)]")
			} else {
				alive = ListNode(reqStrs)
			}
		case "help":
			fmt.Println("Currently, I don't have help instruction, try dig what you want in README. :)")
		default:
			fmt.Println("Unknown command, please check and try again. ")
		}
	}
}

// Establish connection with controller
func createConnection(host string) (*utility.MessageHandler, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Println("ERROR: Can't establish connection with server. Please check server name. ")
		log.Println("ERROR: Can't establish connection with server. Please check server name. ")
		return nil, err
	}
	msgHandler := utility.NewMessageHandler(conn)
	return msgHandler, nil
}
