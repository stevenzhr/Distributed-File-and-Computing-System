package NodeModules

import (
	"dfs/config"
	"dfs/utility"
	"fmt"
	"log"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	flag         bool
	mutex        sync.Mutex
	failTaskList []string
	prqWg        sync.WaitGroup
)

func Heartbeat(wg *sync.WaitGroup) {
	defer wg.Done()
	for flag {
		conn, err := createConnection()
		if err != nil {
			fmt.Println("ERROR: Can't establish connection with server. Node shut down. ")
			flag = false
			return
		}
		msgHandler := utility.NewMessageHandler(conn)
		// wrap heart beat report
		mutex.Lock() // sync lock
		if len(corruptedChunkList) > 0 {
			log.Printf("LOG: Update corrupted chunk list(%s) to contorller. \n", corruptedChunkList)
			failTaskList = append(failTaskList, "cnkCrupt,"+corruptedChunkList) // report correpted chunks
		}
		wrapper := &utility.Wrapper{}
		hb := utility.Heartbeat{
			NodeId:            runConfig.NodeName,
			NodeStatus:        getNodeStatus(),
			AvailableSpace:    getAvailableSpace(runConfig.VaultPath),
			LastAddedFileList: newAddedList,
			Timestemp:         time.Now().Format("2006-01-02 15:04:05"),
			FailTaskList:      failTaskList,
			RequestCount:      uint64(requestCount),
		}
		wrapper.Msg = &utility.Wrapper_HeartbeatMsg{
			HeartbeatMsg: &hb,
		}
		// send heart beat
		log.Println("LOG: send heartBeat report.")
		msgHandler.Send(wrapper)
		newAddedList = make([]string, 0)
		failTaskList = make([]string, 0)
		corruptedChunkList = ""
		mutex.Unlock() //syunc unlock

		// wait response
		resWrapper, err := msgHandler.Receive()
		if err != nil {
			log.Println("WARNING: Unknown error when unpackage form controller. ", err.Error())
			fmt.Println("Unknown error when unpackage form controller. ", err.Error())
			return
		}
		resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
		switch resType {
		case "accept":
			// do nothing
		case "deny":
			fmt.Println("ERROR: HeartBeat report denied. Node shutdown. ")
			log.Println("ERROR: HeartBeat report denied. Node shutdown. ")
			// delete current running config and shutdown node
			deleteRunConfig()
			flag = false
			return
		case "queue":
			// process resQueue
			resQueue := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()
			go processResQueue(resQueue)
		}
		conn.Close()
		// wait for 3s to re-beat
		time.Sleep(config.HEART_BEAT_RATE * time.Second)
	}
}

// repeatedly process each line in resQueue
func processResQueue(resQueue []string) {
	for i := 0; i < len(resQueue); i++ {
		// check := false
		task := strings.Split(resQueue[i], ",")
		switch task[0] {
		case "copyCnk":
			// copy chunk from other nodes
			if len(task) > 1 {
				prqWg.Add(1)
				//TODO: add channel for collect retrieve result
				go retrieveChunk(task[1:])
			}
		case "delCnk":
			// delete chunk for vault
			if len(task) > 1 {
				deleteChunk(task[1:])
			}
		case "shutdown":
			// TODO: start shutdown procedure
		case "init":
			// TODO: start init procedure
		}
		// if !check {
		// 	// if task fail, add current task to fail_task_list
		// 	failTaskList = append(failTaskList, resQueue[i])
		// }
	}
	prqWg.Wait() // wait for all retrieve chunk task finished
	requestCount += len(resQueue)
}

func getNodeStatus() string {
	return "alive"
}

func getAvailableSpace(directory string) uint64 {
	var stat syscall.Statfs_t
	syscall.Statfs(directory, &stat)
	availableSpace := stat.Bavail * uint64(stat.Bsize)
	return availableSpace
}
