package main

import (
    flag "github.com/spf13/pflag"
    "fmt"
    "os"
)

func main() {
    var initCluster bool
    flag.BoolVar(&initCluster, "init-cluster", false,
                 "Node will initialize a cluster.")
    var joinCluster []string
    flag.StringSliceVar(&joinCluster, "join-cluster", nil,
                   "Node will join specified cluster.")

    flag.Parse()

    if initCluster && joinCluster != nil {
        fmt.Println("Hello, either init or join not both.")
        fmt.Println("Hello, fatal error.")
        fmt.Println("Hello, exiting.")
        os.Exit(1)
    }
    if initCluster {
        fmt.Println("Hello, init cluster!")
    } else if joinCluster != nil {
        fmt.Println("Hello, join cluster:")
        for _, ip := range joinCluster {
            fmt.Println(ip)
        }
    } else {
        fmt.Println("Hello, help.")
        flag.Usage()
    }

}
