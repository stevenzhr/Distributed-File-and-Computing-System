package NodeModules

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
	corruptedChunkList string
	requestCount       int
)

func init() {
	corruptedChunkList = ""
	requestCount = 0
}

func CreateListener() (net.Listener, error) {

	listener, err := net.Listen("tcp", ":"+os.Args[2])
	if err != nil {
		fmt.Println("ERROR: Can't create listener. Please check port is available. ")
		log.Println("ERROR: Can't create listener. Please check port is available. ")
		return nil, err
	}
	return listener, nil
}

// start listening for chunk request
func StartListening(listener net.Listener, wg *sync.WaitGroup) {
	defer listener.Close()
	defer wg.Done()
	fmt.Printf("Start listen for connection... \n")
	log.Printf("LOG: Start listen %s for connection... \n", runConfig.Port)

	for flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			requestCount++
			go handleChunkRequest(msgHandler)
		}
	}
	return
}

// handle chunk request
func handleChunkRequest(msgHandler *utility.MessageHandler) {
	// process request
	defer msgHandler.Close()
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println(err.Error())
		return
	}
	// parse chunk request
	if msg, ok := wrapper.Msg.(*utility.Wrapper_RequestMsg); ok {
		if req, ok := msg.RequestMsg.Req.(*utility.Request_ChunkReq); ok {
			chunkReq := req.ChunkReq
			var wrapper *utility.Wrapper
			if chunkReq.GetReq {
				// handle get req
				wrapper = &utility.Wrapper{
					Msg: handleGetChunkReq(chunkReq.ChunkData),
				}
			} else {
				// handle put req
				wrapper = &utility.Wrapper{
					Msg: handlePutChunkReq(chunkReq.ChunkData),
				}
			}
			msgHandler.Send(wrapper)
			return
		} else {
			log.Println("LOG: receive unknown request. ")
		}
	} else {
		log.Println("LOG: receive unknown request. ")
	}
}

func handleGetChunkReq(chunk *utility.Chunk) *utility.Wrapper_ResponseMsg {
	// Create init chunk response
	msg := &utility.Wrapper_ResponseMsg{
		ResponseMsg: &utility.Response{
			Res: &utility.Response_ChunkRes{
				ChunkRes: &utility.ChunkRes{
					Status: false,
				},
			},
		},
	}
	log.Printf("LOG: Handle get chunk(%s) request. ", chunk.FileName)
	chunkPath := runConfig.VaultPath + chunk.FileName
	// check checkSum
	chunkSize, checksum, err := utility.FileInfo(chunkPath)
	if err != nil {
		log.Printf("WARNING: Fail to open chunk file(%s). Chunk not exist. %s", chunkPath, err.Error())
		return msg
	}
	if checksum != chunk.Checksum {
		log.Println("WARNING: Checksum unmatched, local chunk corrupted, delete local chunk file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, chunk.Checksum)
		os.Remove(chunkPath)
		chunkCorruptReport(chunk.FileName)
		return msg
	}
	// Load chunk file
	dataStream, err := ioutil.ReadFile(chunkPath)
	if err != nil {
		log.Println("WARNING: Fail to load chunk file. Chunk may corrupted. ")
		chunkCorruptReport(chunk.FileName)
		return msg
	}
	// return chunk response
	chunk.Size = chunkSize
	chunk.DataStream = dataStream
	msg = &utility.Wrapper_ResponseMsg{
		ResponseMsg: &utility.Response{
			Res: &utility.Response_ChunkRes{
				ChunkRes: &utility.ChunkRes{
					Status:    true,
					ChunkData: chunk,
				},
			},
		},
	}
	log.Println("LOG: Get request finished, send chunk response. ")
	return msg
}

func handlePutChunkReq(chunk *utility.Chunk) *utility.Wrapper_ResponseMsg {
	log.Println("LOG: Handle put chunk request")
	// save chunk file to vault
	fileName := chunk.GetFileName()
	filePath := runConfig.VaultPath + fileName
	// start write chunk to file
	err := ioutil.WriteFile(filePath, chunk.GetDataStream(), 0666)
	if err != nil {
		log.Println("ERROR: Can't create chunkfile. ", err)
		return &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &utility.GeneralRes{
						ResType:     "deny",
						ResponseArg: []string{err.Error()},
					},
				},
			},
		}
	}
	// checksum comparation
	_, checksum, _ := utility.FileInfo(filePath)
	if checksum != chunk.Checksum {
		os.Remove(filePath)
		log.Println("WARNING: Checksum unmatched, delete local chunk file. ")
		//add return fail upload res
		return &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_GeneralRes{
					GeneralRes: &utility.GeneralRes{
						ResType:     "deny",
						ResponseArg: []string{"Checksum unmatched. "},
					},
				},
			},
		}
	}
	// update newAddlist
	newAdd := fileName + "," + checksum
	mutex.Lock() // sync lock
	newAddedList = append(newAddedList, newAdd)
	mutex.Unlock()
	// Pass chunk req to next node
	if len(chunk.PipingList) > 1 {
		chunk.PipingList = chunk.PipingList[1:]
		go passChunk(chunk)
	}
	// prepare response
	log.Println("LOG: Put request finished, send general response. ")
	return &utility.Wrapper_ResponseMsg{
		ResponseMsg: &utility.Response{
			Res: &utility.Response_GeneralRes{
				GeneralRes: &utility.GeneralRes{
					ResType: "accept",
				},
			},
		},
	}
}

// pass chunk by chunk's pipingList TODO: do something to let controller know
func passChunk(chunk *utility.Chunk) {
	host := chunk.PipingList[0]
	log.Println("LOG: Prepare pass chunk to", host)
	// pass chunk
	conn, err := net.Dial("tcp", host)
	if err != nil {
		log.Printf("WARNING: Fail to establish connection with node(%s). Try next candiate. \n", host)
		if len(chunk.PipingList) > 1 {
			chunk.PipingList = chunk.PipingList[1:]
			passChunk(chunk)
			return
		} else {
			log.Printf("ERROR: Fail to establish connection with target node(%s). Chunk passing failed. \n", host)
			// TODO: do something to let controller know
			return
		}
	}
	defer conn.Close()
	nodeMsgHandler := utility.NewMessageHandler(conn)
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &utility.Request{
				Req: &utility.Request_ChunkReq{
					ChunkReq: &utility.ChunkReq{
						GetReq:    false,
						ChunkData: chunk,
					},
				},
			},
		},
	}
	err = nodeMsgHandler.Send(wrapper)
	log.Printf("LOG: Pass chunk to node(%s). \n", host)
	if err != nil {
		log.Printf("WARNING: Connection failed with node(%s). Try next candiate. \n", host)
		if len(chunk.PipingList) > 1 {
			chunk.PipingList = chunk.PipingList[1:]
			log.Printf("LOG: Piping list: %s. \n", chunk.PipingList)
			passChunk(chunk)
			return
		} else {
			log.Printf("ERROR: Connection failed with node(%s). Chunk passing failed. \n", host)
			// TODO: do something to let controller know
			return
		}
	}
	resWrapper, err := nodeMsgHandler.Receive()
	if err != nil {
		log.Println("WARNING: Unknown error when response from node. ", err.Error())
		fmt.Println("WARNING: Unknown error when response from node. ", err.Error())
		return
	}
	resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
	switch resType {
	case "accept":
		return
	default:
		log.Printf("ERROR: Node(%s) denied passing request. Chunk passing failed. \n", host)
		// TODO: do something to let controller know
		return
	}
}

// Repeated delete all chunks in chunkList. If any one failed, return false.
func deleteChunk(chunkList []string) bool {
	log.Println("LOG: Receive delete chunk task. ")
	check := true
	for i := 0; i < len(chunkList); i++ {
		err := os.Remove(runConfig.VaultPath + "/" + chunkList[i])
		if err != nil {
			log.Printf("WARNING: Fail to delete chunk(%s). \n", chunkList[i])
			check = false
		}
		log.Printf("LOG: Delete chunk(%s). \n", chunkList[i])
	}
	log.Printf("LOG: Finish delete chunk task. Return(%t)", check)
	return check
}

// retrieve chunk by chunk's node list
func retrieveChunk(taskArgs []string) bool {
	// taskArgs: [chunkName,checkSum],[node1HostPort],[node2HostPort]
	defer prqWg.Done()
	check := false
	chunkName := taskArgs[0]
	checkSum := taskArgs[1]
	for i := 2; i < len(taskArgs); i++ {
		// prepare ChunkReq
		chunkReq := utility.ChunkReq{
			GetReq: true,
			ChunkData: &utility.Chunk{
				FileName: chunkName,
				Checksum: checkSum,
			},
		}
		// create conn
		host := taskArgs[i]
		log.Printf("LOG: Start connection to retrieve chunk(%s) from node(%s). ", chunkName, host)
		conn, err := net.Dial("tcp", host)
		if err != nil {
			appendStr := ""
			if i < len(taskArgs)-1 { // if last node, no need for append str
				appendStr = ", try next node"
			}
			log.Printf("WARNING: Fail to establish connection with node(%s)%s. \n", host, appendStr)
			continue
		}
		defer conn.Close()
		nodeMsgHandler := utility.NewMessageHandler(conn)
		wrapper := &utility.Wrapper{
			Msg: &utility.Wrapper_RequestMsg{
				RequestMsg: &utility.Request{
					Req: &utility.Request_ChunkReq{
						ChunkReq: &chunkReq,
					},
				},
			},
		}
		// send ChunkReq
		err = nodeMsgHandler.Send(wrapper)
		log.Printf("LOG: Send chunk(%s) request node(%s). \n", chunkName, host)
		if err != nil {
			appendStr := ""
			if i < len(taskArgs)-1 { // if last node, no need for append str
				appendStr = ", try next node"
			}
			log.Printf("WARNING: Connection failed with node(%s)%s. \n", host, appendStr)
			continue
		}
		log.Printf("LOG: Prepare retrieve chunk(%s) from node(%s). ", chunkName, host)
		// Receive ChunkRes
		resWrapper, err := nodeMsgHandler.Receive()
		if err != nil {
			log.Println("WARNING: Unknown error when receive from node. ", err.Error())
			continue
		}
		// check chunk response status
		if resWrapper.GetResponseMsg().GetChunkRes().GetStatus() != true {
			log.Printf("WARNING: Receive denied chunk(%s) request from node(%s). \n", chunkName, host)
			continue
		}
		chunk := resWrapper.GetResponseMsg().GetChunkRes().GetChunkData()
		// save chunk file to vault
		fileName := chunk.GetFileName()
		filePath := runConfig.VaultPath + fileName
		// start write chunk to file
		err = ioutil.WriteFile(filePath, chunk.GetDataStream(), 0666)
		if err != nil {
			log.Printf("ERROR: Can't create chunkfile(%s). %s ", chunkName, err.Error())
			return false
		}
		// run checksum if ok break
		_, checksum, _ := utility.FileInfo(filePath)
		if checksum != chunk.Checksum {
			os.Remove(filePath)
			log.Println("WARNING: Checksum unmatched, delete local chunk file. ")
			log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, chunk.Checksum)
			continue
		} else {
			log.Printf("LOG: Successful retrieve chunk(%s) from node(%s). \n", chunkName, host)
			newAdd := chunkName + "," + checksum
			mutex.Lock()
			newAddedList = append(newAddedList, newAdd)
			mutex.Unlock()
			check = true
			break
		}
	}
	return check
}

func chunkCorruptReport(chunkName string) {
	mutex.Lock()
	if len(corruptedChunkList) == 0 {
		corruptedChunkList += chunkName
	} else {
		corruptedChunkList += "," + chunkName
	}
	mutex.Unlock()
}
