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
	msgHandler     *utility.MessageHandler
	OutputFileName string
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
			msgHandler = utility.NewMessageHandler(conn)
			go handleClientRequest()
			if !flag {
				break
			}
		}
	}
}

func handleClientRequest() {
	// defer msgHandler.Close()
	// start receive client request wrapper
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println("WORNING: possible EOF. ", err.Error())
		return
	}
	// parse request
	var generalRes *utility.GeneralRes
	if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
		if req, ok := msg.RequestMsg.Req.(*utility.Request_MapredReq); ok {
			generalRes = handleMapredRequest(req.MapredReq)
		}
	} else {
		log.Println("WARNING: receive invalid request. ")
		return
	}
	if generalRes != nil {
		resWrapper := &utility.Wrapper{
			Msg: &utility.Wrapper_ResponseMsg{
				ResponseMsg: &utility.Response{
					Res: &utility.Response_GeneralRes{
						GeneralRes: generalRes,
					},
				},
			},
		}
		msgHandler.Send(resWrapper)
		log.Println("LOG: Send response. ")
	}
}

func handleMapredRequest(mapredReq *utility.MapRedReq) *utility.GeneralRes {
	fmt.Printf("Receive MapReduce task(%s) from client. \n", mapredReq.GetInputFile().GetFilename())
	// assume accept response for now
	generalRes := utility.GeneralRes{
		ResType:     "accept",
		ResponseArg: []string{},
	}
	// save output file name
	OutputFileName = mapredReq.OutputName

	// save so file to temp
	soData := mapredReq.GetSoChunk().GetDataStream()
	soFileName := mapredReq.GetSoChunk().GetFileName()
	soFilePath := "temp/" + soFileName
	err := os.MkdirAll("temp/", os.ModePerm)
	if err != nil {
		log.Println("ERROR: Can't create or open temp folder. ", err)
		fmt.Println("ERROR: Can't create or open temp folder. ", err)
		generalRes.ResType = "deny"
		generalRes.ResponseArg = append(generalRes.ResponseArg, "Can't create or open temp folder.")
		return &generalRes
	}
	err = ioutil.WriteFile(soFilePath, soData, 0666)
	if err != nil {
		log.Println("ERROR: Can't create .so file. ", err)
		fmt.Println("ERROR: Can't create .so file. ", err)
		generalRes.ResType = "deny"
		generalRes.ResponseArg = append(generalRes.ResponseArg, "Can't create .so file.")
		return &generalRes
	}
	// check the checksum of .so file
	_, checksum, _ := utility.FileInfo(soFilePath)
	if checksum != mapredReq.GetSoChunk().GetChecksum() {
		os.Remove(soFilePath)
		log.Println("ERROR: Checksum unmatched, delete local .so file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, mapredReq.GetSoChunk().GetChecksum())
		generalRes.ResType = "deny"
		generalRes.ResponseArg = append(generalRes.ResponseArg, ".so file checksum unmatched. ")
		return &generalRes
	} else {
		log.Printf("LOG: Successful retrieve .so file(%s). \n", soFileName)
	}

	mapAssignment := assignNodeWithChunks(mapredReq)

	// init map and reduce tasks
	err = initTasks(mapredReq, soFilePath)
	if err != nil {
		// fail to init task, prepare rollback
		log.Println("ERROR: Fail to init task, MapReduce job stop. ")
		generalRes.ResType = "deny"
		generalRes.ResponseArg = append(generalRes.ResponseArg, "Fail to init task. ")
		return &generalRes
	}
	controllerHost = mapredReq.GetControllerHost()
	log.Println("LOG: Finish init map & reduce task table. ")
	updateMapTasks(mapAssignment)
	log.Println("LOG: Finish update map task table. ")
	updateReduceTasks(mapAssignment)
	log.Println("LOG: Finish update reduce task table. ")

	// add process map & reduce
	assignMapTasks(mapAssignment, mapredReq.GetSoChunk(), mapredReq.GetParameters())
	log.Println("LOG: Finish assign map tasks to node. ")

	// new a connection with workers
	os.Remove(soFilePath)
	// send request to workers
	return &generalRes
}

func sendGeneralResponse(resType string) {
	resWrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &utility.GeneralRes{
						ResType: resType,
					},
				},
			},
		},
	}

	msgHandler.Send(resWrapper)
	log.Printf("LOG: Send response to client -  \"%s\"\n", resType)
	if resType == "complete" {
		msgHandler.Close()
	}
}

func sendGeneralResponseWithArgs(resType string, responseArg []string) {
	resWrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &utility.GeneralRes{
						ResType:     resType,
						ResponseArg: responseArg,
					},
				},
			},
		},
	}

	msgHandler.Send(resWrapper)
	log.Printf("LOG: Send response to client -  \"%s\",  %s\n", resType, responseArg)
	if resType == "complete" {
		msgHandler.Close()
	}
}
