package main

import (
	"fmt"
	"time"
)

var ch chan int

func main() {

	ch = make(chan int, 2)
	go temp()
	time.Sleep(3 * time.Second)
	select {
	case <-ch:
		fmt.Print("channel is not empty")
	default:
		goto end
	}
end:
	fmt.Print("end!")

}

func temp() {
	ch <- 10
	ch <- 100
}
