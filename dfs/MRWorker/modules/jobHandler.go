package workerModules

import (
	"dfs/config"
	"dfs/utility"
	"fmt"
	"io/ioutil"
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
			go handleMapRedRequest(msgHandler)
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
	if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
		switch req := msg.RequestMsg.Req.(type) {
		case *utility.Request_MapTaskReq:
			handleMapTasks(req, msgHandler)
		case *utility.Request_RedTaskReq:
			handleReduceTasks(req, msgHandler)
		case *utility.Request_ChunkReq:
			handleGetPartition(req, msgHandler)
		default:
			log.Println("LOG: Receive unknown type of MapRedReq. ", req)
		}
	}
}

func handleGetPartition(req *utility.Request_ChunkReq, msgHandler *utility.MessageHandler) {
	resMsg := utility.Response{
		Res: &utility.Response_ChunkRes{
			ChunkRes: &utility.ChunkRes{
				Status: false,
			},
		},
	}
	chunk := req.ChunkReq.GetChunkData()
	partitionFileName := chunk.GetFileName()
	log.Printf("LOG: Receive get partition file(%s) request. \n", partitionFileName)
	filepath := fmt.Sprintf("%sws/%s", config.VAULT_PATH, partitionFileName)
	dataStream, err := ioutil.ReadFile(filepath)
	if err != nil {
		wrapper := &utility.Wrapper{
			Msg: &utility.Wrapper_ResponseMsg{
				ResponseMsg: &resMsg,
			},
		}
		err := msgHandler.Send(wrapper)
		if err != nil {
			log.Println("ERROR: Fail to send response. ")
			return
		}
		return
	}
	chunk.DataStream = dataStream
	resMsg = utility.Response{
		Res: &utility.Response_ChunkRes{
			ChunkRes: &utility.ChunkRes{
				Status:    true,
				ChunkData: chunk,
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &resMsg,
		},
	}
	err = msgHandler.Send(wrapper)
	if err != nil {
		log.Println("ERROR: Fail to send response. ")
		return
	}
	log.Printf("LOG: Send partition file(%s) response. \n", partitionFileName)
}
