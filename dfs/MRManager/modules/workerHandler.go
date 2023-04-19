package modules

import (
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	workersMutex sync.RWMutex
	Workers      map[string]string // key: orion01, value: (orion01:workerport)
)

func init() {
	Workers = make(map[string]string)
	MapTasks = make(map[string]TaskStatus)
	ReduceTasks = make(map[string]TaskStatus)
}

func ListenWorkerConn(wg *sync.WaitGroup) {
	flag := true
	listener, err := net.Listen("tcp", ":"+os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			go handleWorkerRequest(msgHandler)
		}
	}
}

func handleWorkerRequest(msgHandler *utility.MessageHandler) {
	defer msgHandler.Close()
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
		if req, ok := msg.RequestMsg.Req.(*utility.Request_JoinReq); ok {
			handleJoin(req.JoinReq.NodeHostPort, msgHandler)
		} else if req, ok := msg.RequestMsg.Req.(*utility.Request_GeneralReq); ok {
			go handleTaskReport(req)
		}
	} else {
		fmt.Println("Invalid request. ")
	}
}

// manager will always accept join request
func handleJoin(hostPort string, msgHandler *utility.MessageHandler) {
	fmt.Println("receive a join request: ", hostPort)
	// add new worker to workers(map)
	hostName := strings.Split(hostPort, ":")[0]
	addNewWorker(hostName, hostPort)

	// prepare response
	res := utility.Response{
		Res: &utility.Response_GeneralRes{
			GeneralRes: &utility.GeneralRes{
				ResType: "accept",
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &res,
		},
	}
	msgHandler.Send(wrapper)
	fmt.Printf("New node(%s) joined. \n", hostPort)
	log.Printf("LOG: New node(%s) joined. \n", hostPort)
}

func addNewWorker(hostName string, hostnPort string) {
	workersMutex.Lock()
	Workers[hostName] = hostnPort
	workersMutex.Unlock()
}
