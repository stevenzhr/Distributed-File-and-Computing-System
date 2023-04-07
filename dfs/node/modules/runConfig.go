package NodeModules

import (
	"fmt"
	"log"
	"os"
)

// load running config file, if no file, start joinServer() process
func LoadRunConfig() error {
	localhost, _ := os.Hostname()
	port := os.Args[2]
	// read local config file, if success, start heartbeat
	// If fail to read local file, read command config and start join process
	wd, _ := os.Getwd()
	configFileName := fmt.Sprintf("%s/runConfig/%s_%s.json", wd, localhost, port)
	ok := runConfig.LoadConfig(configFileName)
	if !ok {
		runConfig.Host = os.Args[1] // Controller host
		runConfig.Port = os.Args[2] // chunk req listen port
		// no config file or can not read file, remove local vault files, start join process
		log.Printf("LOG: Nuke the vault for new start. \n")
		err := nukeVault()
		if err != nil {
			fmt.Println(err.Error())
			log.Printf("WARNING: Can't delete all files in local vault folder. Please check. %s \n", err.Error())
		}
		err = joinServer()
		if err != nil {
			fmt.Println(err.Error())
			log.Println("ERROR: ", err.Error())
			return err
		}
		runConfig.SaveConfig(configFileName)
	}
	// check if vault exist, if not create it
	err := os.MkdirAll(runConfig.VaultPath, os.ModePerm)
	if err != nil {
		log.Println("WARNING: Can't create or open vault folder. ", err)
		return err
	}
	// all good, return null
	return nil
}

func deleteRunConfig() error {
	localhost, _ := os.Hostname()
	port := os.Args[2]
	// read local config file, if success, start heartbeat
	// If fail to read local file, read command config and start join process
	wd, _ := os.Getwd()
	configFileName := fmt.Sprintf("%s/runConfig/%s_%s.json", wd, localhost, port)
	err := os.Remove(configFileName)
	if err != nil {
		log.Println("WARNING: fail to delete local running config file. ")
		return err
	}
	return nil
}

// remove all files in local vault
func nukeVault() error {
	dir, err := os.Open(runConfig.VaultPath)
	if err != nil {
		return err
	}
	defer dir.Close()

	// get file list
	files, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}

	// Delete each file
	remain := false
	for _, file := range files {
		err := os.Remove(runConfig.VaultPath + file)
		if err != nil {
			remain = true
		}
	}
	if remain {
		err := CustomError{message: "File still exists. "}
		return err
	}
	return nil
}
