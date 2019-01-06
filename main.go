package main

import (
    "fmt"
    "time"
    "net"
    "os"
    "io"
    "log"
    "strconv"
    flag "github.com/spf13/pflag"
    node "github.com/curusarn/distributed-system-hw/node"
)

var logFile string = "dsv.log"

func main() {
    var initCluster bool
    flag.BoolVar(&initCluster, "init-cluster", false,
                 "Node will initialize a cluster. (takes precedence over join option)")

    var join string
    flag.StringVar(&join, "join", "",
                   "Join cluster on this IP:port")

    var xxx bool
    flag.BoolVar(&xxx, "xxx", false,
                 "Node will do shit")
    flag.Parse()

    if flag.NArg() != 1 {
        flag.Usage()
        os.Exit(1)
    }

    port, err := strconv.Atoi(flag.Args()[0])
    if err != nil {
        fmt.Print("Port has to be a number")
        flag.Usage()
        os.Exit(1)
    }

    f, err := os.Open("dsv.log")
    if err != nil {
        // fatal
    }
    mw := io.MultiWriter(os.Stdout, f)
    log.SetOutput(mw)
    log.Print("-- log init --")
    log.Print(time.Now())

    n := node.NewNode(GetMyIP(), port)
    n.Print()
    if initCluster {
        fmt.Println("Init cluster!")
        n.InitCluster()
        go n.HeartbeatChecker()
        n.Run()
    } else if join != "" {
        fmt.Println("Join cluster:")
        fmt.Println(join)

        n.Listen()
        n.Join(join)
        go n.HeartbeatChecker()
        if xxx {
            n.RunLeave()
        } else {
            n.Run()
        }
    } else {
        fmt.Println("Use --init-cluster or --join IP:port")
        flag.Usage()
    }


}

// Get preferred outbound ip of this machine
func GetMyIP() net.IP {
    conn, err := net.Dial("udp", "8.8.8.8:80")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    localAddr := conn.LocalAddr().(*net.UDPAddr)

    return localAddr.IP
}
