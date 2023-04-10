package modules

import (
	"dfs/utility"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
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
	// assume accept response for now
	generalRes := utility.GeneralRes{
		ResType: "accept",
	}
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
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
		return nil
	}
	_, checksum, _ := utility.FileInfo(filePath)
	if checksum != mapredReq.GetSoChunk().GetChecksum() {
		os.Remove(filePath)
		log.Println("WARNING: Checksum unmatched, delete local chunk file. ")
		log.Printf("LOG: Checksum: local(%s) vs req(%s) \n", checksum, mapredReq.GetSoChunk().GetChecksum())
		generalRes = utility.GeneralRes{
			ResType: "deny",
		}
	} else {
		log.Printf("LOG: Successful retrieve .so file(%s). \n", fileName)
	}

	mapAssignment := assignNodeWithChunks(mapredReq)

	//TODO: 不用打印，直接用就好
	fmt.Println(mapAssignment)
	// TODO: add process map & reduce
	// new a connection with workers
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

// get a map of (chunk : 3 nodes)
// transfer it into (node : chunks store on it)
// always assign the node with the least number of choices(chunks)
// do not assign the node if it already has more works than average
// return a map of each node and their map chunks
func assignNodeWithChunks(mapredReq *utility.MapRedReq) *map[string][]string {
	// test_4.bin_2-15.cnk:	orion02:26521,orion07:26526,orion06:26525
	chunkNodeList := mapredReq.GetInputFile().GetChunkNodeList()
	chunkNum := len(chunkNodeList)
	// key: test_4.bin_2-15.cnk , value: [orion02:26521, orion07:26526, orion06:26525]

	chunkNodeMap := make(map[string][]string)
	for i := 0; i < len(chunkNodeList); i++ {
		list1 := strings.Split(chunkNodeList[i], ":\t")
		chunkName := list1[0][:len(list1[0])]
		nodeList := strings.Split(list1[1], ",")
		chunkNodeMap[chunkName] = nodeList
	}

	// turn into a (node : chunks) map
	// key: orion02:26521, value: [test_4.bin_2-15.cnk, test_4.bin_3-15.cnk, test_4.bin_4-15.cnk]
	nodeChunkMap := make(map[string][]string)
	for key, value := range chunkNodeMap {
		for i := 0; i < len(value); i++ {
			if _, ok := nodeChunkMap[value[i]]; !ok {
				nodeChunkMap[value[i]] = []string{}
			}
			nodeChunkMap[value[i]] = append(nodeChunkMap[value[i]], key)
		}
	}
	nodeNum := len(nodeChunkMap)

	// always assign the node with least options
	// key: orion02:26521, value : assigned chunk array
	mapAssignment := make(map[string][]string)
	assignedChunkNum := 0
	for assignedChunkNum < chunkNum {
		// find the node with least number of unsigned chunks
		// and the node's assigned chunk number is not greater than avarage
		var minNode string
		minLen := chunkNum
		for key, value := range nodeChunkMap {
			nodeAssignedChunkNum := 0
			if _, ok := mapAssignment[key]; ok {
				nodeAssignedChunkNum = len(mapAssignment[key])
			}
			// if the node has been assigne more chunks than average
			// skip it
			if nodeAssignedChunkNum > assignedChunkNum/nodeNum {
				continue
			}
			if len(value) < minLen {
				minLen = len(value)
				minNode = key
			}
		}

		// assign the first chunk to that node
		if _, ok := mapAssignment[minNode]; !ok {
			mapAssignment[minNode] = []string{}
		}
		chunkName := nodeChunkMap[minNode][0]
		mapAssignment[minNode] = append(mapAssignment[minNode], chunkName)
		assignedChunkNum += 1
		// delete all duplicates of the chunk from the nodeChunkMap
		nodeList := chunkNodeMap[chunkName]
		for _, node := range nodeList {
			for index, chunk := range nodeChunkMap[node] {
				if chunk == chunkName {
					if index == len(nodeChunkMap[node])-1 {
						nodeChunkMap[node] = nodeChunkMap[node][:index]
					} else {
						nodeChunkMap[node] = append(nodeChunkMap[node][:index], nodeChunkMap[node][index+1:]...)
					}
					// delete emply node
					if len(nodeChunkMap[node]) == 0 {
						delete(nodeChunkMap, node)
					}

				}
			}
		}
	}
	// write detailed assignment to log
	log.Println("LOG: Assign each node with these chunks")
	log.Println("Node Name\tMap Task #\t Chunk List")
	for key, value := range mapAssignment {
		log.Printf("%s\t%d\t%s", key, len(value), value)
	}

	return &mapAssignment
}
