package main

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

func main() {
	cmd := exec.Command("bash", "-c", "ping baidu.com")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("%v", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := bufio.NewReader(stdout)
		for {
			readString, err := reader.ReadString('\n')
			if err != nil || err == io.EOF {
				return
			}
			fmt.Print(readString)
		}
	}()

	if err = cmd.Start(); err != nil {
		fmt.Printf("%v", err)
		return
	}
	wg.Wait()
}
