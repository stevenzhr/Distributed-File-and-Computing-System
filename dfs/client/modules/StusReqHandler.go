package ClientModules

import (
	"dfs/utility"
	"fmt"
	"log"
	"sort"
)

func ListFile(reqStr []string) bool {
	reqMsg := utility.Request{
		Req: &utility.Request_StatusReq{
			StatusReq: &utility.StatusReq{
				Type:       "ls",
				RequestArg: reqStr,
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	err := msgHandler.Send(wrapper)
	if err != nil {
		fmt.Println("Connection error when send request to controller. Please exit.")
		log.Printf("WARNING: Connection error when send request to controller. (%s)\n", err.Error())
		return false
	}
	log.Println("LOG: Successfully sent list file request. ")
	//  file-list response
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		fmt.Println("Connection error when unpackage from controller. Please exit. ")
		log.Printf("WARNING: Connection error when unpackage from controller. (%s)\n", err.Error())
		return false
	}
	resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
	switch resType {
	case "accept":
		responseArgs := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()
		log.Println("LOG: accept response. Args type: ", responseArgs[0])
		if responseArgs[0] == "list" {
			fmt.Println("-------File list-------")
			fmt.Println("#   filename    filesize   chunksize")
			//fmt.Print("01  test_5.jpg  1228 MB 3 MB")
			for i := 1; i < len(responseArgs); i++ {
				fmt.Printf("%02d  %s\n", i, responseArgs[i])
			}
		} else if responseArgs[0] == "file" {
			for i := 1; i < len(responseArgs); i++ {
				fmt.Println(responseArgs[i])
			}
		}
	case "deny":
		responseArgs := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()
		log.Println("LOG: deny response. Args: ", responseArgs)
		for i := 0; i < len(responseArgs); i++ {
			fmt.Println(responseArgs[i])
		}
	default:
		log.Println("WARNING: Unknown error when package from controller.(Can't parse response type) ")
		fmt.Println("Unknown error when package from controller.(Can't parse response type) ")
	}
	return true
}

func ListNode(reqStr []string) bool {
	reqMsg := utility.Request{
		Req: &utility.Request_StatusReq{
			StatusReq: &utility.StatusReq{
				Type:       "lsn",
				RequestArg: reqStr,
			},
		},
	}
	wrapper := &utility.Wrapper{
		Msg: &utility.Wrapper_RequestMsg{
			RequestMsg: &reqMsg,
		},
	}
	err := msgHandler.Send(wrapper)
	if err != nil {
		fmt.Println("Connection error when send request to controller. Please exit.")
		log.Printf("WARNING: Connection error when send request to controller. (%s)\n", err.Error())
		return false
	}
	log.Println("LOG: Successfully sent list node request. ")
	//  file-list response
	resWrapper, err := msgHandler.Receive()
	if err != nil {
		fmt.Println("Connection error when unpackage from controller. Please exit. ")
		log.Printf("WARNING: Connection error when unpackage from controller. (%s)\n", err.Error())
		return false
	}
	resType := resWrapper.GetResponseMsg().GetGeneralRes().GetResType()
	switch resType {
	case "accept":
		responseArgs := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()
		log.Println("LOG: accept response. Args: ", responseArgs)
		if responseArgs[0] == "list" {
			fmt.Println("-------------Node list-------------")
			fmt.Println("#   nodeHost                    nodeName     nodeSpace     #OfChunk  reqCount")
			//fmt.Print("11  orion11.cs.usfca.edu:28530  cdbf5104        5018 GB         173     0")
			sort.Strings(responseArgs)
			for i := 1; i < len(responseArgs); i++ {
				fmt.Printf("%02d  %s\n", i, responseArgs[i])
			}
		} else if responseArgs[0] == "node" {
			for i := 1; i < len(responseArgs); i++ {
				fmt.Println(responseArgs[i])
			}
		}

	case "deny":
		responseArgs := resWrapper.GetResponseMsg().GetGeneralRes().GetResponseArg()
		log.Println("LOG: deny response. Args: ", responseArgs)
		for i := 0; i < len(responseArgs); i++ {
			fmt.Println(responseArgs[i])
		}
	default:
		log.Println("WARNING: Unknown error when package from controller.(Can't parse response type) ")
		fmt.Println("Unknown error when package from controller.(Can't parse response type) ")
	}
	return true
}
