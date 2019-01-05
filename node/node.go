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

type NodeRpc struct {
    Node *Node
}

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

    X int
    // topologyBroken bool ??
}

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

func (r *NodeRpc) RpcHeartbeat(arg bool, reply *bool) error {
    r.Node.HeartbeatTs = time.Now()
    r.Node.X = 1
    fmt.Println("got hb at")
    fmt.Println(r.Node.HeartbeatTs)
    return nil
}

func (r *NodeRpc) NonRpcFunc(arg bool) error {
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

func (n *Node) HeartbeatChecker() {
    n.HeartbeatTs = time.Now() // init
    for {
        time.Sleep(heartbeatTimeout)
        ts := n.HeartbeatTs
        fmt.Println("checking hb expiration at")
        fmt.Println(time.Now())
        fmt.Println("> last hb at")
        fmt.Println(ts)
        fmt.Println("> hb expires at")
        heartbeatExpiration := ts.Add(heartbeatTimeout)
        fmt.Println(heartbeatExpiration)
        if time.Now().After(heartbeatExpiration) {
            n.RepairTopology()
        }
    }
}

func Watch(r *NodeRpc) {
    for {
        t := 2000 * time.Millisecond
        time.Sleep(t)
        fmt.Println("X:")
        fmt.Println(r.Node.X)
    }
}

func (n *Node) Listen() {
    r := &NodeRpc {
        Node: n,
    }
    r.Node.X = 0
    rpc.Register(r)
    rpc.HandleHTTP()
    l, e := net.Listen("tcp", ":42420")
    if e != nil {
        log.Fatal("listen error:", e)
        fmt.Println("listen error:", e)
    }
    go http.Serve(l, nil)
    go Watch(r)
}

func (n Node) RepairTopology() {
    fmt.Println("RepairTopology")
}

func (n Node) SendMsg(msg string) bool {
    return true
}
