package main

import (
	"dfs/config"
	"fmt"
	"os"
)

var (
	runConfig config.Config
)

func main() {
	filename := "test.json"
	runConfig.LoadConfig(filename)
	tarPath := runConfig.VaultPath
	err := os.MkdirAll(tarPath, os.ModePerm)
	if err != nil {
		fmt.Println("Can't create or open temp folder. ", err)
		return
	}
	fmt.Println(tarPath)
	runConfig.SaveConfig(tarPath + "test.json")

}
