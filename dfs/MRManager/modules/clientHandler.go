package modules

import (
	"dfs/utility"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
)

var (
	flag           bool
	controllerHost string
)

func ListenClientConn(wg *sync.WaitGroup) {
	flag = true
	listener, err := net.Listen("tcp", ":"+os.Args[2])
	if err != nil {
		log.Println(err.Error())
		return
	}
	for flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			go handleClientRequest(msgHandler)
			if !flag {
				break
			}
		}
	}
}

func handleClientRequest(msgHandler *utility.MessageHandler) {
	defer msgHandler.Close()
	// start receive client request wrapper
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println("WORNING: possible EOF. ", err.Error()) // FIXME:
		return
	}
	// parse request
	var resWrapper *utility.Wrapper
	if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
		if req, ok := msg.RequestMsg.Req.(*utility.Request_MapredReq); ok {
			resWrapper = handleMapredRequest(req.MapredReq)
		}
	} else {
		log.Println("WARNING: receive invalid request. ")
		return
	}
	if resWrapper != nil {
		msgHandler.Send(resWrapper)
		log.Println("LOG: Send response. ")
	}
}

func handleMapredRequest(mapredReq *utility.MapRedReq) *utility.Wrapper {
	fmt.Printf("Receive MapReduce task(%s) from client. \n", mapredReq.GetInputFile().GetFilename())
	// assume accept response for now
	generalRes := utility.GeneralRes{
		ResType: "accept",
	}
	// save so file to temp
	soData := mapredReq.GetSoChunk().GetDataStream()
	soFileName := mapredReq.GetSoChunk().GetFileName()
	soFilePath := "temp/" + soFileName
	err := os.MkdirAll("temp/", os.ModePerm)
	if err != nil {
		log.Println("ERROR: Can't create or open temp folder. ", err)
		fmt.Println("ERROR: Can't create or open temp folder. ", err)
		return nil
	}
	err = ioutil.WriteFile(soFilePath, soData, 0666)
	if err != nil {
		log.Println("ERROR: Can't create .so file. ", err)
		fmt.Println("ERROR: Can't create .so file. ", err)
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
		return nil
	}
	// check the checksum of .so file
	_, checksum, _ := utility.FileInfo(soFilePath)
	if checksum != mapredReq.GetSoChunk().GetChecksum() {
		os.Remove(soFilePath)
		log.Println("ERROR: Checksum unmatched, delete local .so file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, mapredReq.GetSoChunk().GetChecksum())
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
	} else {
		log.Printf("LOG: Successful retrieve .so file(%s). \n", soFileName)
	}

	mapAssignment := assignNodeWithChunks(mapredReq)

	// init map tasks
	err = initTasks(mapredReq, soFilePath)
	if err != nil {
		// TODO: fail to init task, prepare rollback
	}
	controllerHost = mapredReq.GetControllerHost()
	log.Println("LOG: Finish init map & reduce task table. ")
	updateMapTasks(mapAssignment)
	log.Println("LOG: Finish update map task table. ")
	updateReduceTasks(mapAssignment)
	log.Println("LOG: Finish update reduce task table. ")

	// TODO: add process map & reduce
	assignMapTasks(mapAssignment, mapredReq.GetSoChunk(), mapredReq.GetParameters())
	log.Println("LOG: Finish assign map tasks to node. ")

	// new a connection with workers
	os.Remove(soFilePath)
	// send request to workers
	return &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &generalRes,
				},
			},
		},
	}
}
