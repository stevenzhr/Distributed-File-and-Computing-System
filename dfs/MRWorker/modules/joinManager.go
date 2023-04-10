package workerModules

import (
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

// worker to manager connection initialization
func createConnection() (net.Conn, error) {
	host := os.Args[1]
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Println("ERROR: Can't establish connection with manager. Please check host name. ")
		return nil, err
	}
	return conn, nil
}

func JoinServer() error {
	conn, err := createConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	msgHandler := utility.NewMessageHandler(conn)

	// worker send host:port in join request to manager
	log.Println("LOG: Send join request to manager. ")
	localhost, _ := os.Hostname()
	localhost = strings.Split(localhost, ".")[0]
	hostPort := localhost + ":" + os.Args[2]

	reqMsg := utility.Request{
		Req: &utility.Request_JoinReq{
			JoinReq: &utility.JoinReq{
				NodeHostPort: hostPort,
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	msgHandler.Send(wrapper)
	// wait response
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	// check response
	if resWrapper.GetResponseMsg().GetGeneralRes().GetResType() == "accept" {
		// runConfig.NodeName = resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()[0] // get res NodeName
		fmt.Printf("Join request accepted, worker (%s) start waiting for tasks... \n", hostPort)
		log.Printf("LOG: Join request accepted, worker(%s) start waiting for tasks... \n", hostPort)
		return nil
	}
	return err
}
