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
	flag bool
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
	for flag {
		// start receive client request wrapper
		wrapper, err := msgHandler.Receive()
		if err != nil {
			log.Println(err.Error())
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
}

func handleMapredRequest(mapredReq *utility.MapRedReq) *utility.Wrapper {
	// create empty response first
	var generalRes utility.GeneralRes
	// save so file to temp
	soData := mapredReq.GetSoChunk().GetDataStream()
	fileName := mapredReq.GetInputFile().GetFilename() + "_mr.so"
	filePath := "temp/" + fileName
	err := os.MkdirAll("temp/", os.ModePerm)
	if err != nil {
		log.Println("ERROR: Can't create or open temp folder. ", err)
		fmt.Println("ERROR: Can't create or open temp folder. ", err)
		return nil
	}
	err = ioutil.WriteFile(filePath, soData, 0666)
	if err != nil {
		log.Println("ERROR: Can't create .so file. ", err)
		fmt.Println("ERROR: Can't create .so file. ", err)
		return nil
	}
	_, checksum, _ := utility.FileInfo(filePath)
	if checksum != mapredReq.GetSoChunk().GetChecksum() {
		os.Remove(filePath)
		log.Println("WARNING: Checksum unmatched, delete local chunk file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, mapredReq.GetSoChunk().GetChecksum())
	} else {
		log.Printf("LOG: Successful retrieve .so file(%s). \n", fileName)
	}
	// TODO: add process map & reduce

	generalRes = utility.GeneralRes{
		ResType: "accept",
	}
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
