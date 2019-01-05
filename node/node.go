package node

import (
    "fmt"
    "strings"
    "strconv"
    "net/rpc"
    "time"
    "log"
    "net/http"
    "net"
)


const heartbeatTimeout time.Duration = 3000 * time.Millisecond

type NodeRpc int

type Node struct {
    // const
    Uid int64
    Ip string
    Port int

    LeaderUid int64 // leader == uid => I'm leader

    // may use different types later
    NeighbourIp string
    NeighbourPort int

    ParticipatingInElection bool

    HeartbeatTs time.Time

    Rpc NodeRpc
    // topologyBroken bool ??
}

var N *Node

func getUID(ip string, port int) int64 {
    ipSlice := strings.Split(ip, ".")
    var uid int64 = 0
    for _, ipPart := range ipSlice {
        ipPartInt, err := strconv.Atoi(ipPart)
        if err != nil {
            fmt.Print("The end is niegh!!!")
        }
        uid += int64(ipPartInt)
        uid *= 1000
    }
    uid *= 1000
    uid += int64(port)
    fmt.Print("uid: ")
    fmt.Println(uid)
    return uid
}

func New(ip string, port int) Node {
    uid := getUID(ip, port)
    n := Node {
        Uid: uid,
        Ip: ip,
        Port: port,
        LeaderUid: -1,
        NeighbourIp: "",
        NeighbourPort: -1,
        ParticipatingInElection: false,
    }
    return n
}

func (r NodeRpc) RpcHeartbeat(arg bool, reply *bool) error {
    N.HeartbeatTs = time.Now()
    fmt.Println("got hb at")
    fmt.Println(N.HeartbeatTs)
    return nil
}

func (n Node) SendHeartbeat() {
    client, err := rpc.DialHTTP("tcp", n.NeighbourIp + ":42420")
    if err != nil {
        log.Fatal("dialing:", err)
    }
    // Synchronous call
    var reply bool
    err = client.Call("NodeRpc.RpcHeartbeat", true, &reply)
    if err != nil {
        log.Fatal("hb error:", err)
    }
    fmt.Println("hb sent at")
    fmt.Println(time.Now())
}

func (n Node) HeartbeatChecker() {
    N.HeartbeatTs = time.Now() // init
    for {
        time.Sleep(heartbeatTimeout)
        ts := N.HeartbeatTs
        fmt.Println("checking hb expiration at")
        fmt.Println(time.Now())
        fmt.Println("> last hb at")
        fmt.Println(ts)
        fmt.Println("> hb expires at")
        heartbeatExpiration := ts.Add(heartbeatTimeout)
        fmt.Println(heartbeatExpiration)
        if time.Now().After(heartbeatExpiration) {
            N.RepairTopology()
        }
    }
}

func (n Node) Listen() {
    rpc.Register(n.Rpc)
    rpc.HandleHTTP()
    l, e := net.Listen("tcp", ":42420")
    if e != nil {
        log.Fatal("listen error:", e)
        fmt.Println("listen error:", e)
    }
    go http.Serve(l, nil)
}

func (n Node) RepairTopology() {
    fmt.Println("RepairTopology")
}

func (n Node) SendMsg(msg string) bool {
    return true
}
