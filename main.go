package main

import (
    "fmt"
    "time"
    flag "github.com/spf13/pflag"
    node "github.com/curusarn/distributed-system-hw/node"
)

func main() {
    var ip string
    flag.StringVar(&ip, "ip", "127.0.0.1",
                   "Listenning IP address of the node")
    var port int
    flag.IntVar(&port, "port", 42000,
                "Listenning port of the node")
    var initCluster bool
    flag.BoolVar(&initCluster, "init-cluster", false,
                 "Node will initialize a cluster. (takes precedence over join options)")

    var joinIp string
    flag.StringVar(&joinIp, "join-ip", "",
                   "Join cluster on this IP address")
    var joinPort int
    flag.IntVar(&joinPort, "join-port", -1,
                "Join cluster on this port")
    flag.Parse()

    n := node.New(ip, port)
    node.N = &n
    if initCluster {
        fmt.Println("Hello, init cluster!")
        n.LeaderUid = n.Uid // set yourself as leader
        n.NeighbourIp = n.Ip // set yourself as neighbour
        n.NeighbourPort = n.Port
        n.SendMsg("hey")
        n.Listen()
        n.HeartbeatChecker()
        for {
            t := 2000 * time.Millisecond
            time.Sleep(t)
            //n.SendHeartbeat()
        }
    } else if joinIp != "" && joinPort != -1 {
        fmt.Println("Hello, join cluster:")
        fmt.Println(joinIp)
        fmt.Println(joinPort)
        n.NeighbourIp = joinIp // set yourself as neighbour
        n.NeighbourPort = joinPort
        for {
            n.SendHeartbeat()
            t := 2000 * time.Millisecond
            time.Sleep(t)
        }
    } else {
        fmt.Println("Use --init-cluster or --join-xxx")
        flag.Usage()
    }

}
