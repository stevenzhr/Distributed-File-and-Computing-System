package ControllerModules

import (
	"dfs/utility"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

type File struct {
	CheckSum  string            `json:"CheckSum"`
	FileSize  uint64            `json:"FileSize"`
	ChunkSize uint64            `json:"ChunkSize"`
	ChunkMap  map[string]string `json:"ChunkMap"` // key: [chunkName,checksum] value: [node1HostPort,node2HostPort,node3HostPort]
}

var (
	FileMap   map[string]File
	fileMutex sync.RWMutex
)

func init() {
	FileMap = make(map[string]File)
}

func addToFileMap(fileInfo *utility.File, nodeList []string, numOfChunk int) {
	filename := fileInfo.Filename
	// build chunk-node map
	chunkMap := make(map[string]string)
	for i := 0; i < numOfChunk; i++ {
		chunkName := fmt.Sprintf("%s_%d-%d.cnk", filename, i+1, numOfChunk) // if need change, also have to change in clinet
		// set map value to empty, wait heartbeat to add nodeName
		chunkMap[chunkName] = ""
	}
	// create file instance
	file := File{
		CheckSum:  fileInfo.Checksum,
		FileSize:  fileInfo.FileSize,
		ChunkSize: fileInfo.ChunkSize,
		ChunkMap:  chunkMap,
	}
	fileMutex.Lock()
	FileMap[filename] = file
	fileMutex.Unlock()
}

func addToChunkMap(chunkStr string, nodeHost string) {
	// parse filename
	dashIndex := strings.LastIndex(chunkStr, "_")
	if dashIndex != -1 {
		filename := chunkStr[0:dashIndex]
		chunkName := strings.Split(chunkStr, ",")[0]
		fileMutex.Lock()
		if _, exist := FileMap[filename].ChunkMap[chunkName]; exist {
			// delete init chunkName key
			delete(FileMap[filename].ChunkMap, chunkName)
		}
		if len(FileMap[filename].ChunkMap[chunkStr]) == 0 {
			FileMap[filename].ChunkMap[chunkStr] += nodeHost
		} else {
			FileMap[filename].ChunkMap[chunkStr] += "," + nodeHost
		}
		fileMutex.Unlock()
	}
}

func getFile(fileInfo *utility.File) *utility.FileRes {
	status := false
	fileMutex.RLock()
	// Get file meta data from FileMap
	file, exist := FileMap[fileInfo.Filename]
	if exist {
		fileInfo.Checksum = file.CheckSum
		fileInfo.FileSize = file.FileSize
		fileInfo.ChunkSize = file.ChunkSize
		fileInfo.ChunkNodeList = make([]string, len(file.ChunkMap))
		i := 0
		for key, value := range file.ChunkMap {
			fileInfo.ChunkNodeList[i] = key + "," + value
			i++
		}
		status = true
		log.Printf("LOG: File(%s) meta data and chunkNodeList is ready, total chunk num: %d. ", fileInfo.Filename, i)
	} else {
		log.Printf("LOG: Can't find file(%s), warp deny response. ", fileInfo.Filename)
	}
	fileMutex.RUnlock()
	return &utility.FileRes{
		Status:   status,
		FileData: fileInfo,
	}
}

func deleteFile(filename string) bool {
	// delete file
	log.Printf("LOG: Start delete file(%s) process. \n", filename)
	var chunkNodeList []string
	fileMutex.Lock()
	// check file exist in FileMap
	file, exist := FileMap[filename]
	if exist {
		// get chunkNameCksum & nodeList first
		for chunkNameCksum, nodeList := range file.ChunkMap {
			// need str: "chunkName,nodeHost1,nodeHost2,nodeHost3"
			chunkNodeList = append(chunkNodeList, strings.Split(chunkNameCksum, ",")[0]+","+nodeList)
		}
		// delete file in FileMap
		delete(FileMap, filename)
		log.Printf("LOG: Successfully delete file(%s) from controller. Prepare delCnk task to nodes. \n", filename)
	} else {
		// file not exist
		return false
	}
	fileMutex.Unlock()
	// prepare delCnk task for node's ResQueue
	nodeChunkMap := convertToNodeChunkMap(chunkNodeList)
	for nodeHost, chunksStr := range nodeChunkMap {
		// create delCnk str: "delCnk,[chunkName],[chunkName]"
		delCnkStr := "delCnk," + chunksStr
		nodeName := gethashStr(nodeHost)
		nodeMutex.Lock()
		node := NodeMap[nodeName]
		node.ResQueue = append(node.ResQueue, delCnkStr)
		// delete chunk from node chunkList
		node.ChunkList = removeChunk(chunksStr, node.ChunkList)
		NodeMap[nodeName] = node
		nodeMutex.Unlock()
	}
	log.Printf("LOG: Successfully send delCnk task to nodes, %d node has been involved. \n", len(nodeChunkMap))
	return true
}

// Remove all chunks form chunkStr
func removeChunk(chunkStr string, chunkList []string) []string {
	chunks := strings.Split(chunkStr, ",")
	// rename all chunks in chunkStr to "chunkName,checkSum"
	for i := 0; i < len(chunks); i++ {
		for j := 0; j < len(chunkList); j++ {
			if chunks[i] == strings.Split(chunkList[j], ",")[0] {
				chunks[i] = chunkList[j]
				break
			}
		}
		chunkList = removeStrFromArray(chunks[i], chunkList)
	}
	return chunkList
}

func convertToNodeChunkMap(chunkNodeList []string) map[string]string {
	// finish this
	nodeChunkMap := make(map[string]string)
	for i := 0; i < len(chunkNodeList); i++ {
		lineStrs := strings.Split(chunkNodeList[i], ",")
		for j := 1; j < len(lineStrs); j++ {
			if len(nodeChunkMap[lineStrs[j]]) == 0 {
				nodeChunkMap[lineStrs[j]] = lineStrs[0]
			} else {
				nodeChunkMap[lineStrs[j]] += "," + lineStrs[0]
			}
		}
	}
	return nodeChunkMap
}

// Save current nodesMap to local json
func SaveFileMap() {
	jsonBytes, err := json.Marshal(FileMap)
	if err != nil {
		log.Printf("WARNING: Fail to convert nodeMaps data. System may has not backup. ")
	}
	err = ioutil.WriteFile("FilesData.json", jsonBytes, 0644)
	if err != nil {
		log.Printf("WARNING: Can not save nodeMaps data. System may has not backup. ")
	}
}

// Load nodesMap form local json
func LoadFileMap() {
	file, err := os.Open("FilesData.json")
	if err != nil {
		log.Printf("WARNING: Fail to open local file data. ")
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	nodeMutex.Lock()
	err = decoder.Decode(&FileMap)
	nodeMutex.Unlock()
}

func deleteFileMap() {
	err := os.Remove("FilesData.json")
	if err != nil {
		log.Println("WARNING: fail to delete local running config file. ")
		return
	}
	return
}
