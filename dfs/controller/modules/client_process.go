package ControllerModules

import (
	"dfs/config"
	"dfs/utility"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strings"
)

// listen client conn form certain port, use new thread to hand conn
func ListenClientConn() {
	flag := true
	listener, err := net.Listen("tcp", ":"+os.Args[2])
	if err != nil {
		log.Println(err.Error())
		return
	}
	for flag {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := utility.NewMessageHandler(conn)
			go handleClientRequest(msgHandler)
		}
	}
}

// handle client request. parse request and call corresponding func to process req
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
			if req, ok := msg.RequestMsg.Req.(*utility.Request_FileReq); ok {
				switch req.FileReq.GetReqType() {
				case "put":
					resWrapper = handlePutFileReq(req.FileReq.GetFileInfo())
				case "get":
					resWrapper = handleGetFileReq(req.FileReq.GetFileInfo())
				case "delete":
					resWrapper = handleDelFileReq(req.FileReq.GetFileInfo())
				default:
					log.Println("WARNING: Recieve an unknown file request. ")
				}
			} else if req, ok := msg.RequestMsg.Req.(*utility.Request_StatusReq); ok {
				resWrapper = handleStatusReq(req.StatusReq)
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

func handlePutFileReq(fileInfo *utility.File) *utility.Wrapper {
	log.Println("LOG: receive client put file req")
	// create empty response first
	var generalRes utility.GeneralRes
	// check file existance in system
	filename := fileInfo.GetFilename()
	fileSize := fileInfo.GetFileSize()
	_, exist := FileMap[filename]
	if exist {
		generalRes = utility.GeneralRes{
			ResType:     "deny",
			ResponseArg: []string{"Find exist file, please delete first."},
		}
	} else if fileSize > getAvaliableSpace(true) {
		// check current free space and compare with size
		generalRes = utility.GeneralRes{
			ResType:     "deny",
			ResponseArg: []string{"File too large."},
		}
	} else {
		// file check all good, calculate number of splits
		chunkSize := fileInfo.GetChunkSize()
		numOfChunk := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
		// create chunk-node list
		log.Printf("LOG: Upload file check done. Prepare node list response. #chunk: %d, chunksize: %d \n", numOfChunk, chunkSize)
		// FIXME: decide generate nodeList strategy
		nodeList := getNodeListBySep(filename, numOfChunk, chunkSize)
		// log.Println("LOG: Assigned node list: ", nodeList)
		// add file info to FileMap
		addToFileMap(fileInfo, nodeList, numOfChunk)
		SaveFileMap()
		// create general response
		generalRes = utility.GeneralRes{
			ResType:     "accept",
			ResponseArg: nodeList,
		}
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

// get all nodes' avaliable space and divide by 3, return by bytes
func getAvaliableSpace(showAppending bool) uint64 {
	var avaliableSpace uint64
	nodeMutex.RLock()
	for _, node := range NodeMap {
		avaliableSpace += node.AvailableSpace
		if showAppending {
			// chunk who is appending to upload
			for _, chunkSize := range node.appendantChunkMap {
				avaliableSpace += chunkSize
			}
		}
	}
	nodeMutex.RUnlock()
	return avaliableSpace/3 - config.RESERVED_SPACE
}

// use load balanced strategy to return each chunk's target node list
func getNodeListByLodBlc(filename string, numOfChunk int, chunkSize uint64) []string {
	// Load balanced strategy
	resNodeList := make([]string, numOfChunk)
	for i := 0; i < int(numOfChunk); i++ {
		nodeMutex.RLock()
		// get first 3 empty nodes
		nodeList := make([]string, len(NodeMap))
		j := 0
		for k := range NodeMap {
			nodeList[j] = k
			j++
		}
		sort.Slice(nodeList, func(m, n int) bool {
			return getNodeReservedSapce(NodeMap[nodeList[m]]) < getNodeReservedSapce(NodeMap[nodeList[n]])
		})
		nodeList = nodeList[:3]
		for j := 0; j < 3; j++ {
			// add appending chunk to node's appendantChunkMap
			chunkName := fmt.Sprintf("%s_%d-%d.cnk", filename, j+1, numOfChunk) // if need change, also have to change in clinet
			if NodeMap[nodeList[j]].appendantChunkMap == nil {
				// find empty map, init
				node := NodeMap[nodeList[j]]
				node.appendantChunkMap = make(map[string]uint64)
				NodeMap[nodeList[j]] = node
			}
			NodeMap[nodeList[j]].appendantChunkMap[chunkName] = chunkSize

			nodeList[j] = NodeMap[nodeList[j]].NodeHost
		}
		nodeMutex.RUnlock()
		resNodeList[i] = fmt.Sprintf("%s,%s,%s", nodeList[0], nodeList[1], nodeList[2])
	}
	return resNodeList
}

// use distribution balanced strategy to return each chunk's target node list
func getNodeListBySep(filename string, numOfChunk int, chunkSize uint64) []string {
	resNodeList := make([]string, numOfChunk)
	i := 0
	nodeMutex.RLock()
	nodeList := make([]string, len(NodeMap))
	// get all available node list
	for _, node := range NodeMap {
		nodeList[i] = node.NodeHost
		i++
	}
	nodeMutex.RUnlock()
	numOfNodes := len(nodeList)
	for i := 0; i < len(resNodeList); i++ {
		resNodeList[i] = fmt.Sprintf("%s,%s,%s", nodeList[(i)%numOfNodes], nodeList[(i+1)%numOfNodes], nodeList[(i+2)%numOfNodes])
	}
	return resNodeList
}

// return node's avaliable space + total appending chunk size, return by bytes
func getNodeReservedSapce(node Node) uint64 {
	var appendingSize uint64
	for _, size := range node.appendantChunkMap {
		appendingSize += size
	}
	return node.AvailableSpace + appendingSize
}

// pass fileInfo to getFile() and wrap the reply fileRes
func handleGetFileReq(fileInfo *utility.File) *utility.Wrapper {
	log.Println("LOG: receive client get file req")
	fileRes := getFile(fileInfo)
	return &utility.Wrapper{
		Msg: &utility.Wrapper_ResponseMsg{
			ResponseMsg: &utility.Response{
				Res: &utility.Response_FileRes{
					FileRes: fileRes,
				},
			},
		},
	}
}

// check file exist, if so call deleteFile()  //TODO: under constructing
func handleDelFileReq(fileInfo *utility.File) *utility.Wrapper {
	log.Printf("LOG: receive client delete file(%s) request. \n", fileInfo.Filename)
	// create empty response first
	var generalRes utility.GeneralRes
	// check exist
	fileMutex.RLock()
	_, exist := FileMap[fileInfo.Filename]
	fileMutex.RUnlock()
	if exist && deleteFile(fileInfo.Filename) {
		generalRes = utility.GeneralRes{
			ResType: "accept",
		}
	} else if !exist {
		generalRes = utility.GeneralRes{
			ResType:     "deny",
			ResponseArg: []string{"No such file."},
		}
	} else {
		generalRes = utility.GeneralRes{
			ResType:     "deny",
			ResponseArg: []string{"Controller fail to delete the file, please try again."},
		}
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

func handleStatusReq(statusReq *utility.StatusReq) *utility.Wrapper {
	log.Println("LOG: receive client status req")
	var wrapper *utility.Wrapper
	switch statusReq.Type {
	case "ls":
		wrapper = handleListFileReq(statusReq.RequestArg)
	case "lsn":
		wrapper = handleListNodeReq(statusReq.RequestArg)
	default:
		return nil
	}
	return wrapper
}

func handleListFileReq(requestArgs []string) *utility.Wrapper {
	// requestArgs: "ls example.txt -c"
	var generalRes utility.GeneralRes
	if len(requestArgs) == 1 {
		// General list file req, return all files
		// return format: ["filename \t filesize \t chunksize"]
		fileMutex.RLock()
		fileList := make([]string, len(FileMap)+1)
		fileList[0] = "list"
		i := 1
		for filename, file := range FileMap {
			fileSize := uint64(math.Ceil(float64(file.FileSize) / 1024 / 1024))
			// chunkSize := uint64(math.Ceil(float64(file.ChunkSize) / 1024 / 1024))
			formattedChunkSize := ""
			if file.ChunkSize > 1024*1024 {
				formattedChunkSize = fmt.Sprintf("%d MB", file.ChunkSize/1024/1024)
			} else if file.ChunkSize > 1024 {
				formattedChunkSize = fmt.Sprintf("%d KB", file.ChunkSize/1024)
			} else {
				formattedChunkSize = fmt.Sprintf("%d byte", file.ChunkSize)
			}
			fileList[i] = fmt.Sprintf("%s\t%d MB\t%s", filename, fileSize, formattedChunkSize)
			i++
		}
		fileMutex.RUnlock()
		generalRes = utility.GeneralRes{
			ResType:     "accept",
			ResponseArg: fileList,
		}
	} else {
		resStrArr := make([]string, 3)
		resStrArr[0] = "file"
		// Single file req, return file meta data
		filename := requestArgs[1]
		fileMutex.RLock()
		if file, exist := FileMap[filename]; exist {
			fileSize := uint64(math.Ceil(float64(file.FileSize) / 1024 / 1024))
			// chunkSize := uint64(math.Ceil(float64(file.ChunkSize) / 1024 / 1024))
			formattedChunkSize := ""
			if file.ChunkSize > 1024*1024 {
				formattedChunkSize = fmt.Sprintf("%d MB", file.ChunkSize/1024/1024)
			} else if file.ChunkSize > 1024 {
				formattedChunkSize = fmt.Sprintf("%d KB", file.ChunkSize/1024)
			} else {
				formattedChunkSize = fmt.Sprintf("%d byte", file.ChunkSize)
			}
			// check chunkNum is complete
			intact := ">>>Damaged<<<"
			numOfChunk := int(math.Ceil(float64(file.FileSize) / float64(file.ChunkSize)))
			if len(file.ChunkMap) == numOfChunk {
				intact = "Completed"
			}
			// return format: ["filename \t filesize \t chunksize"]
			resStrArr[1] = fmt.Sprintf("%s size: %d MB chunkSize: %s %s", filename, fileSize, formattedChunkSize, intact)
			// return format: ["checksum: (hash)"]
			resStrArr[2] = fmt.Sprintf("checksum: %s", file.CheckSum)
			if len(requestArgs) == 3 && requestArgs[2] == "-c" {
				// get chunk list
				// return format: "chunkFilename: orion01,orion02,orion03"
				i := 0
				chunklist := make([]string, len(file.ChunkMap))
				for chunkNC, nodeList := range file.ChunkMap {
					chunkName := strings.Split(chunkNC, ",")[0]
					nodeListStr := strings.ReplaceAll(nodeList, ".cs.usfca.edu", "")
					chunklist[i] = fmt.Sprintf("%s:\t%s", chunkName, nodeListStr)
					i++
				}
				resStrArr = append(resStrArr, chunklist...)
			}
			generalRes = utility.GeneralRes{
				ResType:     "accept",
				ResponseArg: resStrArr,
			}
		} else {
			generalRes = utility.GeneralRes{
				ResType:     "deny",
				ResponseArg: []string{"No such file. "},
			}
		}
		fileMutex.RUnlock()
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

func handleListNodeReq(requestArgs []string) *utility.Wrapper {
	var generalRes utility.GeneralRes
	// return format: ["nodeName \t nodeHost \t nodeSpace \t numOfChunk \t reqCount"]
	if len(requestArgs) == 1 {
		nodeMutex.RLock()
		nodeList := make([]string, len(NodeMap)+1)
		nodeList[0] = "list"
		i := 1
		for nodeName, node := range NodeMap {
			nodeSpace := uint64(math.Ceil(float64(node.AvailableSpace) / 1024 / 1024 / 1024))
			nodeList[i] = fmt.Sprintf("%s\t%s\t%d GB \t%d\t%d", node.NodeHost, nodeName, nodeSpace, len(node.ChunkList), node.RequestCount)
			i++
		}
		nodeMutex.RUnlock()
		generalRes = utility.GeneralRes{
			ResType:     "accept",
			ResponseArg: nodeList,
		}
	} else {
		nodeName := requestArgs[1]
		resStrArr := make([]string, 2)
		resStrArr[0] = "node"
		resStrArr[1] = fmt.Sprintf("Chunks in node %s:", nodeName)
		// Single file req, return file meta data
		nodeMutex.RLock()
		if node, exist := NodeMap[nodeName]; exist {
			chunkList := node.ChunkList
			for i := 0; i < len(chunkList); i++ {
				chunkList[i] = strings.Split(chunkList[i], ",")[0]
			}
			resStrArr = append(resStrArr, chunkList...)
			generalRes = utility.GeneralRes{
				ResType:     "accept",
				ResponseArg: resStrArr,
			}
		} else {
			generalRes = utility.GeneralRes{
				ResType:     "deny",
				ResponseArg: []string{"No such node. "},
			}
		}
		nodeMutex.RUnlock()
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
