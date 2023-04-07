package NodeModules

import (
	. "dfs/config"
	"dfs/utility"
	"fmt"
	"log"
	"net"
	"os"
)

var (
	runConfig    Config
	ChunkList    map[string]Chunk // may turn into private for blockProcess()
	newAddedList []string
)

type Chunk struct {
	blockName string
	size      string
	checkSum  string
}

type CustomError struct {
	message string
}

func init() {
	flag = true
	ChunkList = make(map[string]Chunk)
	newAddedList = make([]string, 0)
	failTaskList = make([]string, 0)
}

// Node to controller connection initialization
func createConnection() (net.Conn, error) {
	host := os.Args[1]
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Println("ERROR: Can't establish connection with server. Please check server name. ")
		return nil, err
	}
	return conn, nil
}

func joinServer() error {
	conn, err := createConnection()
	if err != nil {
		return err
	}
	defer conn.Close()
	msgHandler := utility.NewMessageHandler(conn)

	// fmt.Println("Send join request to controller...")
	log.Println("LOG: Send join request to controller. ")
	localhost, _ := os.Hostname()
	reqMsg := utility.Request{
		Req: &utility.Request_JoinReq{
			JoinReq: &utility.JoinReq{
				NodeHostPort: localhost + ":" + runConfig.Port,
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
	// fmt.Println("Wait for response...")
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	// check response
	if resWrapper.GetResponseMsg().GetGeneralRes().GetResType() == "accept" {
		runConfig.NodeName = resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()[0] // get res NodeName
		fmt.Printf("Join request accepted, node(%s) start working... \n", runConfig.NodeName)
		log.Printf("LOG: Join request accepted, node(%s) start working... \n", runConfig.NodeName)
		return nil
	} else {
		log.Println("ERROR: Join request denied, program terminate. ")
		err := CustomError{message: "Join request denied."}
		return err
	}
}

func (e CustomError) Error() string {
	return fmt.Sprintf("custom error: %s", e.message)
}
