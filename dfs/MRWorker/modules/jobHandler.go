package workerModules

import (
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"os"
)

var (
	Flag bool
)

func CreateListener() (net.Listener, error) {

	listener, err := net.Listen("tcp", ":"+os.Args[2])
	if err != nil {
		fmt.Println("ERROR: Can't create listener. Please check port is available. ")
		log.Println("ERROR: Can't create listener. Please check port is available. ")
		return nil, err
	}
	return listener, nil
}

func StartListening(listener net.Listener) {
	defer listener.Close()
	fmt.Printf("Start listen for connection... \n")
	log.Printf("LOG: Start listen %s for connection... \n", os.Args[2])

	for Flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			handleMapRedRequest(msgHandler)
		}
	}

}

func handleMapRedRequest(msgHandler *utility.MessageHandler) {
	// process request
	defer msgHandler.Close()
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println(err.Error())
		return
	}
}
