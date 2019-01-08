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

type LampartsClock struct {
    Time int64
    Lock sync.RWMutex
}

func (c *LampartsClock) Get() int64 {
    c.Lock.RLock()
    t := c.Time
    c.Lock.RUnlock()
    return t
}

func (c *LampartsClock) GetAndInc() int64 {
    c.Lock.RLock()
    c.Time++
    t := c.Time
    c.Lock.RUnlock()
    return t
}

func (c *LampartsClock) SetIfHigherAndInc(t int64) {
    c.Lock.Lock()
    if c.Time < t {
        c.Time = t
    }
    c.Time++
    c.Lock.Unlock()
}

func (c *LampartsClock) Inc() {
    c.Lock.Lock()
    c.Time++
    c.Lock.Unlock()
}


type Msg interface {
    GetLogicalTime() int64
    SetLogicalTime(int64)
}

type LogicalTime struct {
    LogicalTime int64
}

func (l LogicalTime) GetLogicalTime() int64 {
    return l.LogicalTime
}

func (l *LogicalTime) SetLogicalTime(t int64) {
    l.LogicalTime = t
}

type BaseMsg struct {
    LogicalTime
    Ttl int
}

type ReplyMsg struct {
    LogicalTime
    Success bool
}

func getBaseMsg() BaseMsg {
    return BaseMsg {Ttl: initTtl}
}

type UidMsg struct {
    BaseMsg
    Uid int64
}

type AddrMsg struct {
    BaseMsg
    Addr string
}

type NodeMsg struct {
    BaseMsg
    Uid int64
    Addr string
}

type TwoAddrMsg struct {
    AddrMsg
    NewAddr string
}

type VarMsg struct {
    NodeMsg
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

func (r *NodeRpc) Heartbeat(msg BaseMsg, reply *ReplyMsg) error {
    r.Node.heartbeatTs = time.Now()
    log.Print("Recieved Heartbeat")
    return nil
}

func (r *NodeRpc) Join(msg AddrMsg, reply *NodeMsg) error {
    log.Print("Recieved Join from ", msg.Addr)
    // what if I do not have a neighbour ?
    reply.Uid = r.Node.leaderUid // pass leader uid to joining node

    r.Node.LockMtx()
    reply.Addr = r.Node.neighbourAddr
    r.Node.neighbourAddr = msg.Addr
    r.Node.DialNeighbour()
    // if dial fails set reply.neighbourAddr to ""
    // or return error
    r.Node.UnlockMtx()
    return nil
}

func (r *NodeRpc) Leave(msg TwoAddrMsg, reply *ReplyMsg) error {
    log.Print("Recieved Leave from ", msg.Addr)
    // what if I do not have a neighbour ?
    if msg.Addr != r.Node.neighbourAddr {
        r.Node.FwdLeave(&msg, reply)
        return nil
    }
    r.Node.LockMtx()
    r.Node.neighbourAddr = msg.NewAddr
    r.Node.DialNeighbour()
    // if dial fails set reply.neighbourAddr to ""
    // or return error
    r.Node.UnlockMtx()
    return nil
}

func (r *NodeRpc) Repair(msg AddrMsg, reply *ReplyMsg) error {
    log.Print("Recieved Repair from ", msg.Addr)
    log.Print("Neighbour: ", r.Node.neighbourAddr)

    err := r.Node.FwdRepair(&msg, reply)
    if err != nil {
        // failed FwdRepair -> we are the last node
        r.Node.LockMtx()
        r.Node.neighbourAddr = msg.Addr
        r.Node.DialNeighbour()
        // if dial fails set reply to false 
        // or return error
        r.Node.UnlockMtx()
        log.Print("Topology Repaired")
    }
    return nil
}

func (r *NodeRpc) Vote(msg UidMsg, reply *ReplyMsg) error {
    if msg.Uid > r.Node.uid {
        r.Node.FwdVote(&msg, reply)
        return nil
    } else if msg.Uid < r.Node.uid {
        if r.Node.participatingInElection == false {
            msg.Uid = r.Node.uid
            r.Node.FwdVote(&msg, reply)
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

func (r *NodeRpc) ElectedMsg(msg UidMsg, reply *ReplyMsg) error {
    if msg.Uid != r.Node.uid {
        r.Node.FwdElectedMsg(&msg, reply)
        r.Node.leaderUid = msg.Uid
        r.Node.participatingInElection = false
    }
    return nil
}

func (r *NodeRpc) Read(msg UidMsg, reply *VarMsg) error {
    log.Print("Recieved Read ", msg)
    if msg.Uid == r.Node.uid {
        log.Print("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.StartVote()
        return errors.New("No leader - starting election")
    }
    if r.Node.leaderUid == r.Node.uid {
        log.Print("Leader recv Read")
        // i'm the leader
        reply.SharedVariable = r.Node.sharedVariable
        r.Node.StartBroadcast()
        return nil
    }
    err, value := r.Node.FwdRead(&msg, reply)
    if err != nil {
        // handle errors
        log.Print("ReadRpc error 8899")
    }
    reply.SharedVariable = value
    return nil
}

func (r *NodeRpc) Write(msg VarMsg, reply *ReplyMsg) error {
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
    r.Node.FwdWrite(&msg, reply)
    return nil
}

func (r *NodeRpc) Broadcast(msg VarMsg, reply *ReplyMsg) error {
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
    r.Node.FwdBroadcast(&msg, reply)
    return nil
}

func Watch(r *NodeRpc) {
    for {
        t := 2000 * time.Millisecond
        time.Sleep(t)
    }
}

// Node

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

    logicalClock *LampartsClock
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
        logicalClock: new(LampartsClock),
    }
    return n
}

func (n node) getBaseMsg() BaseMsg {
    return BaseMsg {LogicalTime {0}, initTtl}
    // LogicalTime is being set in SendMsg()
}

func (n node) getUidMsg() UidMsg {
    return UidMsg {n.getBaseMsg(), n.uid}
}

func (n node) getAddrMsg() AddrMsg {
    return AddrMsg {n.getBaseMsg(), n.addr}
}

func (n node) getNodeMsg() NodeMsg {
    return NodeMsg {n.getBaseMsg(), n.uid, n.addr}
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


func (n node) _sendMsg(method string, args Msg, reply Msg) error {
    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        return errors.New("No neighbour: neighbourRpc == nil")
    }
    args.SetLogicalTime(n.logicalClock.GetAndInc())

    err := n.neighbourRpc.Call(method, args, reply)
    if err == nil {
        n.logicalClock.SetIfHigherAndInc(reply.GetLogicalTime())
    }
    n.RUnlockMtx()
    return err
}

func (n node) SendMsg(method string, args Msg, reply Msg) error {
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
    msg := n.getBaseMsg()
    var reply ReplyMsg
    err := n.SendMsg("Heartbeat", &msg, &reply)
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

    var reply NodeMsg
    log.Print("Joining")
    err = client.Call("NodeRpc.Join", n.getNodeMsg(), &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }
    n.logicalClock.SetIfHigherAndInc(reply.GetLogicalTime())

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
    var reply ReplyMsg
    msg := TwoAddrMsg {n.getAddrMsg(), n.neighbourAddr}
    log.Print("Starting Leave ", msg)
    err := n.SendMsg("Leave", &msg, &reply)
    if err != nil {
        log.Print("Leave error:", err)
    }

    n.LockMtx()
    n.neighbourAddr = ""
    n.neighbourRpc = nil
    n.UnlockMtx()

    log.Print("Left")
}

func (n node) FwdLeave(msg *TwoAddrMsg, reply *ReplyMsg) error {
    log.Print("Forwarding Leave")
    return n.SendMsg("Leave", msg, reply)
}

func (n node) StartRepair() error {
    var reply ReplyMsg
    msg := n.getAddrMsg()
    log.Print("Starting Repair ", msg)
    err := n.SendMsg("Repair", &msg, &reply)
    if err != nil {
        log.Print("Repair error:", err)
        return err
    }
    return err
}

func (n node) FwdRepair(msg *AddrMsg, reply *ReplyMsg) error {
    log.Print("Forwarding Repair")
    return n.SendMsg("Repair", msg, reply)
}

// Vote ElectedMsg

func (n node) StartVote() error {
    var reply ReplyMsg
    msg := n.getUidMsg()
    log.Print("Starting Vote ", msg)
    err := n.SendMsg("Vote", &msg, &reply)
    if err != nil {
        log.Print("Vote error:", err)
        return err
    }
    n.participatingInElection = true
    return err
}

func (n node) FwdVote(msg *UidMsg, reply *ReplyMsg) error {
    log.Print("Forwarding Vote ", msg)
    err := n.SendMsg("Vote", msg, reply)
    if err == nil {
        n.participatingInElection = true
    }
    return err
}

func (n node) StartElectedMsg() error {
    var reply ReplyMsg
    msg := UidMsg {n.getBaseMsg(), n.uid}
    log.Print("Starting ElectedMsg ", msg)

    err := n.SendMsg("ElectedMsg", &msg, &reply)
    if err != nil {
        log.Print("ElectedMsg error:", err)
        return err
    }
    n.participatingInElection = false
    return err
}

func (n node) FwdElectedMsg(msg *UidMsg, reply *ReplyMsg) error {
    log.Print("Forwarding ElectedMsg ", msg)

    err := n.SendMsg("ElectedMsg", msg, reply)
    if err == nil {
        n.participatingInElection = true
    }
    return err
}


// Read Write Broadcast

func (n *node) StartRead() (error, int) {
    var reply VarMsg
    msg := n.getUidMsg()
    log.Print("Starting Read ", msg)
    err := n.SendMsg("Read", &msg, &reply)
    if err != nil {
        log.Print("Read error: ", err)
    }
    return err, reply.SharedVariable
}

func (n *node) FwdRead(msg *UidMsg, reply *VarMsg) (error, int) {
    log.Print("Forwarding read ", msg)
    err := n.SendMsg("Read", msg, reply)
    if err == nil {
        n.sharedVariable = reply.SharedVariable
    }
    return err, reply.SharedVariable
}

func (n node) StartWrite(value int) error {
    var reply ReplyMsg
    msg := VarMsg {n.getNodeMsg(), value}
    log.Print("Starting Write ", msg)
    err := n.SendMsg("Write", &msg, &reply)
    if err != nil {
        log.Print("Write error: ", err)
        return err
    }
    log.Print("Write successful: ")
    return err
}

func (n node) FwdWrite(msg *VarMsg, reply *ReplyMsg) error {
    log.Print("Forwarding Write ", msg)
    return n.SendMsg("Write", msg, reply)
}

func (n node) StartBroadcast() {
    var reply ReplyMsg
    msg := VarMsg {n.getNodeMsg(), n.sharedVariable}
    log.Print("Starting Broadcast ", msg)
    err := n.SendMsg("Broadcast", &msg, &reply)
    if err != nil {
        log.Print("Broadcast error: ", err)
    }
}

func (n node) FwdBroadcast(msg *VarMsg, reply *ReplyMsg) error {
    log.Print("Forwarding Broadcast ", msg)
    return n.SendMsg("Broadcast", msg, reply)
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

