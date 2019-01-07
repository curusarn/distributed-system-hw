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
    "errors"
    "sync"
)

const heartbeatTimeout time.Duration = 10 * time.Second
const retryDelay time.Duration = 100 * time.Millisecond
const retryCount int = 3
const initTtl int = 10

type NodeInfo struct {
    Uid int64
    Addr string
}

type TwoNodeInfo struct {
    NodeInfo
    NewUid int64
    NewAddr string
}

type VariableMsg struct {
    NodeInfo
    SharedVariable int
}

type RpcMsg struct {
    Ttl int // always
    FromUid int64 // always
    FromIpPort string // always
    NewUid int64 // leave only
    NewIpPort string // leave only
    Value int // broadcast & write
}

type RpcReply struct {
    Success bool // 
    Uid int64
    IpPort string
    Value int
}

// NodeRpc

type NodeRpc struct {
    Node *node
}

func (r *NodeRpc) Heartbeat(_ bool, _ *bool) error {
    r.Node.heartbeatTs = time.Now()
    log.Print("Recieved Heartbeat")
    return nil
}

func (r *NodeRpc) Join(info NodeInfo, reply *NodeInfo) error {
    log.Print("Recieved Join from ")
    log.Print(info.Addr)
    // what if I do not have a neighbour ?
    reply.Uid = r.Node.leaderUid // pass leader uid to joining node
    r.Node.LockMtx()
    reply.Addr = r.Node.neighbourAddr
    r.Node.neighbourAddr = info.Addr
    r.Node.DialNeighbour()
    // if dial fails set reply.neighbourAddr to ""
    // or return error
    r.Node.UnlockMtx()
    return nil
}

func (r *NodeRpc) Leave(info TwoNodeInfo, reply *bool) error {
    log.Print("Recieved Leave from ")
    log.Print(info.Addr)
    // what if I do not have a neighbour ?
    if info.Addr != r.Node.neighbourAddr {
        r.Node.FwdLeave(info)
        return nil
    }
    r.Node.LockMtx()
    r.Node.neighbourAddr = info.NewAddr
    r.Node.DialNeighbour()
    // if dial fails set reply.neighbourAddr to ""
    // or return error
    r.Node.UnlockMtx()
    return nil
}

func (r *NodeRpc) Repair(info NodeInfo, reply *bool) error {
    log.Print("Recieved Repair from ")
    log.Print(info.Addr)
    log.Print("Neighbour: ", r.Node.neighbourAddr)

    err := r.Node.FwdRepair(info)
    if err != nil {
        // failed FwdRepair -> we are the last node
        r.Node.LockMtx()
        r.Node.neighbourAddr = info.Addr
        r.Node.DialNeighbour()
        // if dial fails set reply to false 
        // or return error
        r.Node.UnlockMtx()
        log.Print("Topology Repaired")
    }
    return nil
}

func (r *NodeRpc) Vote(uid int64, reply *bool) error {
    if uid > r.Node.uid {
        r.Node.FwdVote(uid)
        return nil
    } else if uid < r.Node.uid {
        if r.Node.participatingInElection == false {
            r.Node.FwdVote(r.Node.uid)
        }
        return nil
    }
    // elected
    r.Node.leaderUid = r.Node.uid
    r.Node.participatingInElection = false
    // post election
    r.Node.StartElectedMsg()
    return nil
}

func (r *NodeRpc) ElectedMsg(uid int64, reply *bool) error {
    if uid != r.Node.uid {
        r.Node.FwdElectedMsg(uid)
        r.Node.leaderUid = uid
        r.Node.participatingInElection = false
    }
    return nil
}

func (r *NodeRpc) Read(info NodeInfo, reply *int) error {
    log.Print("Recieved Read ", info)
    if info.Uid == r.Node.uid {
        log.Print("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.StartVote()
        return errors.New("No leader - starting election")
    }
    if r.Node.leaderUid == r.Node.uid {
        log.Print("Leader recv Read")
        // i'm the leader
        *reply = r.Node.sharedVariable
        r.Node.StartBroadcast()
        return nil
    }
    err, value := r.Node.FwdRead(info)
    if err != nil {
        // handle errors
        log.Print("ReadRpc error 8899")
    }
    *reply = value
    return nil
}

func (r *NodeRpc) Write(msg VariableMsg, reply *bool) error {
    log.Print("Recieved Write ", msg)
    if msg.Uid == r.Node.uid {
        log.Print("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.StartVote()
        return errors.New("No leader - starting election")
    }
    if r.Node.leaderUid == r.Node.uid {
        log.Print("Leader recv Write")
        // i'm the leader
        r.Node.sharedVariable = msg.SharedVariable
        r.Node.StartBroadcast()
        return nil
    }
    r.Node.FwdWrite(msg)
    return nil
}

func (r *NodeRpc) Broadcast(msg VariableMsg, reply *bool) error {
    log.Print("Recieved Broadcast ", msg)
    if r.Node.leaderUid == r.Node.uid {
        log.Print("Leader recv Broadcast")
        // i'm the leader
        // end Broadcast
        return nil
    }
    if msg.Uid == r.Node.uid {
        log.Print("Broadcast ended at original node")
        // full roundtrip
        // end broadcast 
        return nil
    }
    // set sharedVariable
    r.Node.sharedVariable = msg.SharedVariable
    r.Node.FwdBroadcast(msg)
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
}

type node struct {
    // const
    uid int64
    addr string // socket addres

    neighbourAddr string
    neighbourRpc *rpc.Client
    neighbourMtx *sync.RWMutex

    leaderUid int64 // leader == uid => I'm leader
    participatingInElection bool

    heartbeatTs time.Time

    sharedVariable int
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
        neighbourMtx: new(sync.RWMutex),
    }
    return n
}

func (n node) getNodeInfo() NodeInfo {
    return NodeInfo {n.uid, n.addr}
}

func (n *node) LockMtx() {
    n.neighbourMtx.Lock()
}

func (n *node) UnlockMtx() {
    n.neighbourMtx.Unlock()
}

func (n *node) RLockMtx() {
    n.neighbourMtx.RLock()
}

func (n *node) RUnlockMtx() {
    n.neighbourMtx.RUnlock()
}

func (n node) Print() {
    log.Print("uid: " + strconv.Itoa(int(n.uid)))
    log.Print("addr: " + n.addr)
}

func (n node) PrintState() {
    log.Print("neigh: " + n.neighbourAddr)
    log.Print("leader: " + strconv.Itoa(int(n.leaderUid)))
    log.Print("shrVar: " + strconv.Itoa(n.sharedVariable))
}

func (n *node) InitCluster() {
    n.LockMtx()
    n.leaderUid = n.uid // set yourself as leader
    n.neighbourAddr = n.addr // set yourself as neighbour

    n.sharedVariable = 420

    n.Listen()
    n.DialNeighbour()
    // Abort if dial fails  
    // or return error
    n.UnlockMtx()
}

func (n *node) DialNeighbour() {
    // TODO handle failed dials better
    client, err := rpc.DialHTTP("tcp", n.neighbourAddr)
    if err != nil {
        log.Fatal("dialing:", err)
    }
    n.neighbourRpc = client
}


func (n node) _sendMsg(method string,
                       args interface{},
                       reply interface{}) error {
    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        return errors.New("No neighbour: neighbourRpc == nil")
    }
    err := n.neighbourRpc.Call(method, args, reply)
    n.RUnlockMtx()
    return err
}

func (n node) SendMsg(method string,
                      args interface{},
                      reply interface{}) error {
    var err error
    serviceMethod := "NodeRpc." + method

    for i := 0; i < retryCount; i++ {
        err = n._sendMsg(serviceMethod, args, reply)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        log.Print("_sendMsg error: ", err)
        // multiple reply delays ?
        time.Sleep(retryDelay)
    }
    if err != nil {
        // this will probably go away or become debug only
        log.Print("SendMsg error: ", err)
    }
    return err
}

// Heartbeat

func (n node) Heartbeat() {
    var reply bool
    err := n.SendMsg("Heartbeat", true, &reply)
    if err != nil {
        log.Print("Heartbeat error: ", err)
    }
}

// Join Leave Repair

func (n *node) Join(addr string) {
    // dial before call ... duh
    client, err := rpc.DialHTTP("tcp", addr)
    if err != nil {
        log.Fatal("dialing:", err)
        // retry?
        // then Fatal
    }

    var reply NodeInfo
    log.Print("Joining")
    err = client.Call("NodeRpc.Join", n.getNodeInfo(), &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }

    if reply.Addr == n.addr {
        // do not accept self as neighbour
        log.Print("Joined broken ring - expecting repair")
        return
    }

    n.LockMtx()
    n.neighbourAddr = reply.Addr
    n.DialNeighbour()
    n.UnlockMtx()

    n.leaderUid = reply.Uid

    log.Print("Joined")
}

func (n node) StartLeave() {
    log.Print("Starting Leave")
    var reply bool
    msg := TwoNodeInfo {n.getNodeInfo(), 0, n.neighbourAddr}
    err := n.SendMsg("Leave", msg, &reply)
    if err != nil {
        log.Print("Leave error:", err)
    }

    n.LockMtx()
    n.neighbourAddr = ""
    n.neighbourRpc = nil
    n.UnlockMtx()

    log.Print("Left")
}

func (n node) FwdLeave(msg TwoNodeInfo) error {
    log.Print("Forwarding Leave")
    var reply bool
    return n.SendMsg("Leave", msg, &reply)
}

func (n node) StartRepair() error {
    log.Print("Starting Repair")
    var reply bool
    msg := n.getNodeInfo()
    err := n.SendMsg("Repair", msg, &reply)
    if err != nil {
        log.Print("Repair error:", err)
        return err
    }
    return err
}

func (n node) FwdRepair(msg NodeInfo) error {
    log.Print("Forwarding Repair")
    var reply bool
    return n.SendMsg("Repair", msg, &reply)
}

// Vote ElectedMsg

func (n node) StartVote() error {
    var reply bool
    log.Print("Starting Vote ", n.uid)
    err := n.SendMsg("Vote", n.uid, &reply)
    if err != nil {
        log.Print("Vote error:", err)
        return err
    }
    n.participatingInElection = true
    return err
}

func (n node) FwdVote(uid int64) error {
    var reply bool
    log.Print("Forwarding Vote ", uid)
    err := n.SendMsg("Vote", n.uid, &reply)
    if err == nil {
        n.participatingInElection = true
    }
    return err
}

func (n node) StartElectedMsg() error {
    var reply bool
    log.Print("Starting ElectedMsg ", n.uid)

    err := n.SendMsg("ElectedMsg", n.uid, &reply)
    if err != nil {
        log.Print("ElectedMsg error:", err)
        return err
    }
    n.participatingInElection = false
    return err
}

func (n node) FwdElectedMsg(uid int64) error {
    var reply bool
    log.Print("Forwarding ElectedMsg ", uid)

    err := n.SendMsg("ElectedMsg", uid, &reply)
    if err == nil {
        n.participatingInElection = true
    }
    return err
}


// Read Write Broadcast

func (n *node) StartRead() (error, int) {
    log.Print("Starting Read")
    var reply int
    msg := n.getNodeInfo()
    err := n.SendMsg("Read", msg, &reply)
    if err != nil {
        log.Print("Read error: ", err)
    }
    return err, reply
}

func (n *node) FwdRead(msg NodeInfo) (error, int) {
    log.Print("Forwarding read ", msg)
    var reply int
    err := n.SendMsg("Read", msg, &reply)
    if err == nil {
        n.sharedVariable = reply
    }
    return err, reply
}

func (n node) StartWrite(value int) error {
    log.Print("Starting Write ", value)
    var reply bool
    msg := VariableMsg {n.getNodeInfo(), value}
    err := n.SendMsg("Write", msg, &reply)
    if err != nil {
        log.Print("Write error: ", err)
        return err
    }
    log.Print("Write successful: ")
    return err
}

func (n node) FwdWrite(msg VariableMsg) error {
    log.Print("Forwarding Write ", msg)
    var reply bool
    return n.SendMsg("Write", msg, &reply)
}

func (n node) StartBroadcast() {
    log.Print("Starting Broadcast ", n.sharedVariable)
    var reply bool
    msg := VariableMsg {n.getNodeInfo(), n.sharedVariable}
    err := n.SendMsg("Broadcast", msg, &reply)
    if err != nil {
        log.Print("Broadcast error: ", err)
    }
}

func (n node) FwdBroadcast(msg VariableMsg) error {
    log.Print("Forwarding Broadcast ", msg)
    var reply bool
    return n.SendMsg("Broadcast", msg, &reply)
}

// more functions

func (n *node) HeartbeatChecker() {
    n.heartbeatTs = time.Now() // init
    for {
        time.Sleep(heartbeatTimeout)
        ts := n.heartbeatTs
        heartbeatExpiration := ts.Add(heartbeatTimeout)
        if time.Now().After(heartbeatExpiration) {
            n.StartRepair()
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

func (n *node) Run() {
    for {
        for i := 0; i < 3; i++ {
            log.Print("ready.")
            n.Heartbeat()
            time.Sleep(5000 * time.Millisecond)
        }
        n.PrintState()
        time.Sleep(5000 * time.Millisecond)
    }
}

func (n *node) RunLeave() {
    for {
        for j := 0; j < 10; j++ {
            for i := 0; i < 2; i++ {
                log.Print("ready.")
                n.Heartbeat()
                time.Sleep(5000 * time.Millisecond)
            }
            n.PrintState()
            time.Sleep(5000 * time.Millisecond)
            err, value := n.StartRead()
            if err != nil {
                log.Print("Read failed")
            } else {
                log.Print("Read: ", value)
            }
            if j == 3 {
                log.Print("Write: 111")
                n.StartWrite(111)
            }
        }
        n.StartLeave()
        return
    }
}

