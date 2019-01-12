package main

import (
    "bufio"
    "errors"
    "fmt"
    "time"
    "net"
    "os"
    "io"
    "log"
    "strconv"
    "strings"
    flag "github.com/spf13/pflag"
    node "github.com/curusarn/distributed-system-hw/node"
)

var logFile string = "dsv.log"
var joinCluster string

func main() {
    var initCluster bool
    flag.BoolVar(&initCluster, "init-cluster", false,
                 "Node will initialize a cluster." +
                 " (takes precedence over join option)")

    flag.StringVar(&joinCluster, "join", "",
                   "Join cluster on this IP:port")
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

    f, err := os.Create(logFile)
    if err != nil {
        fmt.Println("Error while creating log file:", err)
        os.Exit(2)
    }
    mw := io.MultiWriter(os.Stdout, f)
    logger := log.New(mw, "", log.Ldate|log.Ltime)
    logger.SetOutput(mw)
    logger.Print("-- log init --")

    n := node.NewNode(GetMyIP(), port, logger)
    n.Listen()
    if initCluster {
        fmt.Println("Init cluster!")
        err := n.InitCluster()
        if err != nil {
            log.Fatal("InitCluster failed")
        }
        ProcessStdin(n)
    } else if joinCluster != "" {
        fmt.Println("Join cluster:")
        fmt.Println(joinCluster)
        err := n.Join(joinCluster)
        if err != nil {
            log.Fatal("Join failed")
        }
        ProcessStdin(n)
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

func ProcessStdin(n node.Node) {
    reader := bufio.NewReader(os.Stdin)
    var line string
    var err error
    for {
        line, err = reader.ReadString('\n')
        if err != nil {
            fmt.Println("CMD ERR != nil")

            break
        }

        // Process the line here.
        // split by space (max 3 parts) 
        line = strings.TrimSuffix(line, "\n")
        slice := strings.SplitN(line, " ", 3)[:2]
        if len(slice) > 2 {
            fmt.Println("CMD ERR: more than 2 words")
        }
        if len(slice) < 1 {
            fmt.Println("CMD WARN: less than 1 word")
        }
        cmd := slice[0]
        var arg string
        if len(slice) > 1 {
            arg = slice[1]
        }
        err = RunCmd(n, cmd, arg)

    }
    if err != io.EOF {
        fmt.Println("CMD io ERROR:", err)
    }
}

func RunCmd(n node.Node, cmd string, arg string) error {
    fmt.Println("CMD", cmd, arg)
    argPresent := (arg != "")
    var argValue, value int
    var err error
    if argPresent {
        argValue, err = strconv.Atoi(arg)
        if err != nil {
            return err
        }
    }
    switch cmd {
    case "sleep":
        // sleep for 'arg' seconds
        //fmt.Println("CMD sleep", argValue)
        if argPresent == false {
            argValue = 1 // default arg 1
        }
        fmt.Println("Sleeping for", arg, "seconds")
        time.Sleep(time.Second * time.Duration(argValue))
    case "write":
        //fmt.Println("CMD write", argValue)
        if argPresent == false {
            argValue = 42 // default arg 42
        }
        err = n.Write(argValue)
        if err != nil {
            fmt.Println("Write failed!")
            return err
        }
    case "read":
        //fmt.Println("CMD read")
        value, err = n.Read()
        if err != nil {
            fmt.Println("CMD Read failed!")
            return err
        }
        fmt.Println("CMD Read sharedVariable =", value)
    case "leave":
        // leave properly
        //fmt.Println("CMD leave")
        err = n.Leave()
        if err != nil {
            fmt.Println("CMD Leave failed!")
            return err
        }
        fmt.Println("CMD Leave successful")
        //os.Exit(0)
    case "quit":
        // leave w/o message
        //fmt.Println("CMD quit")
        n.LeaveWithoutMsg()
        fmt.Println("CMD Quit successful - left the cluster without message")
        //os.Exit(0)
    case "join":
        // join the cluster
        //fmt.Println("CMD join")
        if joinCluster == "" {
            fmt.Println("CMD Join failed - was initializing node!")
            return err
        }
        err = n.Join(joinCluster)
        if err != nil {
            fmt.Println("CMD Join failed!")
            return err
        }
    case "info":
        // print info
        n.Print()
        n.PrintState()
        return nil
    case "":
        fmt.Println("CMD Skipping empty command")
        return nil
    default:
        fmt.Println("CMD Can't process cmd", cmd)
        return errors.New("CMD Unrecognized command")
    }
    return nil
}
