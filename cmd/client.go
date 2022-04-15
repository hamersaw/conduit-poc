package main

import (
	"fmt"

	//"google.golang.org/grpc"
	//"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("Hello Client!")

	/*conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()*/
}
