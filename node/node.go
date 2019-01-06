package node

import (
    "fmt"
    "strconv"
    "strings"
    "net/rpc"
    "time"
    "log"
    "net/http"
    "net"
)

const heartbeatTimeout time.Duration = 3000 * time.Millisecond

type NodeInfo struct {
    Uid int64
    Addr string
}

type TwoNodeInfo struct {
    NodeInfo
    NewUid int64
    NewAddr string
}

// NodeRpc

type NodeRpc struct {
    Node *node
}

func (r *NodeRpc) Heartbeat(arg bool, reply *bool) error {
    r.Node.heartbeatTs = time.Now()
    log.Print("got hb")
    return nil
}

func (r *NodeRpc) Join(info NodeInfo, reply *NodeInfo) error {
    log.Print("Recieved Join from ")
    log.Print(info.Addr)
    // what if I do not have a neighbour ?
    reply.Uid = 0
    reply.Addr = r.Node.neighbourAddr
    r.Node.neighbourAddr = info.Addr
    r.Node.DialNeighbour()
    return nil
}

func (r *NodeRpc) Leave(info TwoNodeInfo, reply *bool) error {
    log.Print("Recieved Leave from ")
    log.Print(info.Addr)
    // what if I do not have a neighbour ?
    if info.Addr != r.Node.neighbourAddr {
        r.Node.ForwardLeave(info)
        return nil
    }
    r.Node.neighbourAddr = info.NewAddr
    r.Node.DialNeighbour()
    return nil
}

func (r *NodeRpc) NonRpcFunc(arg bool) error {
    return nil
}


func Watch(r *NodeRpc) {
    for {
        t := 2000 * time.Millisecond
        time.Sleep(t)
    }
}

// Node

// do i need this ?
type Node interface {
    InitCluster()
    Listen()
}

type node struct {
    // const
    uid int64
    addr string // socket addres

    leaderUid int64 // leader == uid => I'm leader

    neighbourAddr string
    neighbourRpc *rpc.Client

    participatingInElection bool

    heartbeatTs time.Time

    // topologyBroken bool ??
}

func getUid(ip net.IP, port int) int64 {
    var uid int64 = 0
    for _, ipPart := range ip {
        uid += int64(ipPart)
        uid *= 1000
    }
    uid *= 1000
    uid += int64(port)
    fmt.Print("Node uid: ")
    fmt.Println(uid)
    return uid
}

func getIpPort(ip net.IP, port int) string {
    return ip.String() + ":" + strconv.Itoa(port)
}

func NewNode(ip net.IP, port int) node {
    n := node {
        uid: getUid(ip, port),
        addr: getIpPort(ip, port),
    }
    return n
}

func (n node) getNodeInfo() NodeInfo {
    return NodeInfo {n.uid, n.addr}
}

func (n node) Print() {
    log.Print("uid: " + strconv.Itoa(int(n.uid)))
    log.Print("addr: " + n.addr)
}

func (n node) PrintState() {
    log.Print("neigh: " + n.neighbourAddr)
    log.Print("leader: " + strconv.Itoa(int(n.leaderUid)))
}

func (n *node) InitCluster() {
    n.leaderUid = n.uid // set yourself as leader
    n.neighbourAddr = n.addr // set yourself as neighbour
}

func (n *node) DialNeighbour() {
    client, err := rpc.DialHTTP("tcp", n.neighbourAddr)
    if err != nil {
        log.Fatal("dialing:", err)
    }
    n.neighbourRpc = client
}

func (n *node) Join(addr string) {
    // dial before call ... duh
    client, err := rpc.DialHTTP("tcp", addr)
    if err != nil {
        log.Fatal("dialing:", err)
    }

    var reply NodeInfo
    log.Print("Join ...")
    err = client.Call("NodeRpc.Join", n.getNodeInfo(), &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }
    log.Print("join atempt")

    n.neighbourAddr = reply.Addr
    n.DialNeighbour()

    log.Print("joined")
}

func (n node) Leave() {
    // Synchronous call
    info := TwoNodeInfo {n.getNodeInfo(), 0, n.neighbourAddr}
    var reply bool
    log.Print("Leave ...")
    err := n.neighbourRpc.Call("NodeRpc.Leave", info, &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }

    n.neighbourAddr = ""
    n.neighbourRpc = nil

    log.Print("Left")
}

func (n node) ForwardLeave(info TwoNodeInfo) {
    // Synchronous call
    var reply bool
    log.Print("ForwardLeave ...")
    err := n.neighbourRpc.Call("NodeRpc.Leave", info, &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }
    log.Print("ForwardLeave done")
}

func (n *node) Run() {
    for {
        for i := 0; i < 3; i++ {
            log.Print("ready.")
            n.SendHeartbeat()
            time.Sleep(2000 * time.Millisecond)
        }
        n.PrintState()
        time.Sleep(2000 * time.Millisecond)
    }
}

func (n *node) RunLeave() {
    for {
        for i := 0; i < 5; i++ {
            for i := 0; i < 3; i++ {
                log.Print("ready.")
                n.SendHeartbeat()
                time.Sleep(2000 * time.Millisecond)
            }
            n.PrintState()
            time.Sleep(2000 * time.Millisecond)
        }
        n.Leave()
        return
    }
}

func (n node) SendHeartbeat() {
    // Synchronous call
    var reply bool
    err := n.neighbourRpc.Call("NodeRpc.Heartbeat", true, &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }
    log.Print("hb sent")
}

func (n *node) HeartbeatChecker() {
    n.heartbeatTs = time.Now() // init
    for {
        time.Sleep(heartbeatTimeout)
        ts := n.heartbeatTs
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

func (n *node) Listen() {
    r := &NodeRpc {
        Node: n,
    }
    rpc.Register(r)
    rpc.HandleHTTP()
    port := ":" + strings.Split(n.addr, ":")[1]
    l, e := net.Listen("tcp", port)
    if e != nil {
        log.Fatal("listen error:", e)
        fmt.Println("listen error:", e)
    }
    go http.Serve(l, nil)
}

func (n node) RepairTopology() {
    fmt.Println("RepairTopology")
}

func (n node) SendMsg(msg string) bool {
    return true
}
