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
    lampartsclock "github.com/curusarn/distributed-system-hw/lampartsclock"
)

// send Heartbeat every
const heartbeatInterval time.Duration = 3 * time.Second
// require Heartbeat every
const heartbeatTimeout time.Duration = 10 * time.Second
// how long to wait before retry
const retryDelay time.Duration = 100 * time.Millisecond
// how many times to retry  
const retryCount int = 3
// how many hops can message make before being discarded
// prevents messages from staying in the ring forver 
//      if recipient node leaves the ring
// NOTE: ttl has to be higher than number of nodes
const initTtl int = 10

type Msg interface {
    GetLogicalTime() int64
    SetLogicalTime(int64)
    DecTtl() error
}

//type ReplyMsg interface {
//    GetLogicalTime() int64
//    SetLogicalTime(int64)
//}

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

func (m *BaseMsg) DecTtl() error {
    m.Ttl--
    if m.Ttl <= 0 {
        return errors.New("Message ttl is equal to zero")
    }
    return nil
}

type ReplyMsg struct {
    LogicalTime
    Success bool
    // TODO remove this and use ReplyMsg as interface
}

func (m ReplyMsg) DecTtl() error {
    return nil
    // possibly separate ReplyMsg from Msg interface
    // and remove this func
    // TODO
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

// NodeRpc
// RPC wrapper for node struct
type NodeRpc struct {
    Node *node
}

func (r *NodeRpc) Heartbeat(msg AddrMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.heartbeatTs = time.Now()
    r.Node.logPrint("Recieved Heartbeat from", msg.Addr)

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Join(msg AddrMsg, reply *NodeMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Join from", msg.Addr)
    // what if I do not have a neighbour ?
    reply.Uid = r.Node.leaderUid // pass leader uid to joining node

    reply.Addr = r.Node.neighbourAddr
    err := r.Node.DialNeighbour(msg.Addr)
    if err != nil {
        r.Node.logPrint("NodeRpc: Join request declined!")
        err = errors.New("NodeRpc: Join request declined!")
        reply.Addr = ""
    }

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Leave(msg TwoAddrMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Leave from", msg.Addr)
    // what if I do not have a neighbour ?
    if msg.Addr != r.Node.neighbourAddr {
        r.Node.FwdLeave(&msg, reply)
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }

    err := r.Node.DialNeighbour(msg.NewAddr)
    if err != nil {
        r.Node.logPrint("NodeRpc: Leave request declined!")
        err = errors.New("NodeRpc: Leave request declined!")
    }

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Repair(msg AddrMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Repair from ", msg.Addr)
    r.Node.logPrint("Neighbour: ", r.Node.neighbourAddr)

    err := r.Node.FwdRepair(&msg, reply)
    if err != nil {
        r.Node.logPrint("Topology Repaired")
        // failed FwdRepair -> we are the last node
        err := r.Node.DialNeighbour(msg.Addr)
        if err != nil {
            err = errors.New("NodeRpc: Repair request declined!")
        } else {
            r.Node.logPrint("Topology Repaired")
        }
    }
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Vote(msg UidMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    if msg.Uid > r.Node.uid {
        r.Node.FwdVote(&msg, reply)
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    } else if msg.Uid < r.Node.uid {
        if r.Node.participatingInElection == false {
            msg.Uid = r.Node.uid
            r.Node.FwdVote(&msg, reply)
        }
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    // elected
    r.Node.leaderUid = r.Node.uid
    r.Node.participatingInElection = false
    // post election
    r.Node.ElectedMsg()

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) ElectedMsg(msg UidMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    if msg.Uid != r.Node.uid {
        r.Node.FwdElectedMsg(&msg, reply)
        r.Node.leaderUid = msg.Uid
        r.Node.participatingInElection = false
    }
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Read(msg UidMsg, reply *VarMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Read ", msg)
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.Vote() // this is blocking - we might just repeat the read
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return errors.New("No leader - starting election")
    }
    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recv Read")
        // i'm the leader
        reply.SharedVariable = r.Node.sharedVariable
        r.Node.Broadcast()
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    value, err := r.Node.FwdRead(&msg, reply)
    if err != nil {
        // handle errors
        r.Node.logPrint("ReadRpc error 8899")
    }
    reply.SharedVariable = value
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Write(msg VarMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Write ", msg)
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.Vote() // this is blocking - we might just repeat the read
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return errors.New("No leader - starting election")
    }
    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recv Write")
        // i'm the leader
        r.Node.sharedVariable = msg.SharedVariable
        r.Node.Broadcast()
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    r.Node.FwdWrite(&msg, reply)
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Broadcast(msg VarMsg, reply *ReplyMsg) error {
    if r.Node.leftTheCluster {
        return nil
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Broadcast ", msg)
    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recv Broadcast")
        // i'm the leader
        // end Broadcast
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("Broadcast ended at original node")
        // full roundtrip
        // end broadcast 
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    // set sharedVariable
    r.Node.sharedVariable = msg.SharedVariable
    r.Node.FwdBroadcast(&msg, reply)
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func Watch(r *NodeRpc) {
    for {
        t := 2000 * time.Millisecond
        time.Sleep(t)
    }
}

// Node
type Node interface {
    InitCluster() error
    Join(str string) error
    Leave() error
    LeaveWithoutMsg()
    Read() (int, error)
    Write(int) error
    Print()
    PrintState()
}

type node struct {
    // const
    uid int64
    addr string // socket addres (ip:port)

    server *http.Server

    // mtx protected access
    neighbourAddr string
    neighbourRpc *rpc.Client
    neighbourMtx *sync.RWMutex

    // TODO make atomic
    leaderUid int64 // leader == uid => I'm leader
    participatingInElection bool

    // TODO make atomic
    heartbeatTs time.Time

    // TODO make atomic
    sharedVariable int

    logicalClock *lampartsclock.LampartsClock

    // TODO create mtx 
    logger *log.Logger

    // TODO make atomic
    leftTheCluster bool
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

func NewNode(ip net.IP, port int, logger *log.Logger) *node {
    n := new(node)
    n.uid = getUid(ip, port)
    n.addr = getIpPort(ip, port)
    n.neighbourMtx = new(sync.RWMutex)
    n.logicalClock = new(lampartsclock.LampartsClock)
    n.logger = logger
    n.leftTheCluster = false
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

func (n *node) logPrint(str ...interface{}) {
    t := strconv.Itoa(int(n.logicalClock.Get()))
    n.logger.Print("|" + t + "| ", str)
}

func (n node) Print() {
    fmt.Println("uid: ", n.uid)
    fmt.Println("addr: ", n.addr)
}

func (n node) PrintState() {
    fmt.Println("neighbour:", n.neighbourAddr)
    fmt.Println("leader:", n.leaderUid)
    fmt.Println("shraredVar:", n.sharedVariable)
    fmt.Println("inElection:", n.participatingInElection)
    fmt.Println("leftCluster:", n.leftTheCluster)
}

func (n *node) InitCluster() error {
    //n.Listen()
    n.sharedVariable = -1 // init sharedVariable to -1 
    n.leaderUid = n.uid // set yourself as leader
    err := n.DialNeighbour(n.addr) // set yourself as neighbour and dial
    if err != nil {
        n.logPrint("InitCluster error - can't connect to self - very wierd!")
        return err
    }
    // Abort if dial fails  
    // or return error

    go n.HeartbeatChecker()
    go n.HeartbeatSender()
    return nil
}

func (n *node) _dialNeighbour(addr string) (*rpc.Client, error) {
    client, err := rpc.DialHTTP("tcp", addr)
    if err != nil {
        return client, err
    }
    return client, err
}

func (n *node) DialNeighbour(newAddr string) error {
    n.logPrint("Dialing", newAddr)
    var client *rpc.Client
    var err error
    for i := 0; i < retryCount; i++ {
        client, err = n._dialNeighbour(newAddr)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        n.logPrint("_dialNeighbour error: ", err)
        // multiple reply delays ?
        time.Sleep(retryDelay)
    }
    if err != nil {
        n.logPrint("Dialing neighbour ", newAddr,
                   " failed:", err)
        return err
    }

    if n.neighbourRpc != nil {
        n.neighbourRpc.Close()
        n.neighbourRpc = nil
    }
    n.neighbourAddr = newAddr
    n.neighbourRpc = client
    n.logPrint("Dialing succes")
    return nil
}


func (n node) _sendMsg(method string, msg Msg, reply Msg) error {
    n.RLockMtx()
    defer n.RUnlockMtx()
    if n.neighbourRpc == nil {
        // n.logPrint("No neighbourRpc")
        return errors.New("No neighbour: neighbourRpc == nil")
    }
    msg.SetLogicalTime(n.logicalClock.GetAndInc())

    err := n.neighbourRpc.Call(method, msg, reply)
    if err == nil {
        n.logicalClock.SetIfHigherAndInc(reply.GetLogicalTime())
    }
    return err
}

func (n node) SendMsg(method string, msg Msg, reply Msg) error {
    var err error
    serviceMethod := "NodeRpc." + method
    err = msg.DecTtl()
    if err != nil {
        // this will probably go away or become debug only
        n.logPrint("SendMsg error: ", err)
        return err
    }

    for i := 0; i < retryCount; i++ {
        err = n._sendMsg(serviceMethod, msg, reply)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        n.logPrint("_sendMsg error: ", err)
        // multiple reply delays ?
        time.Sleep(retryDelay)
    }
    if err != nil {
        // this will probably go away or become debug only
        n.logPrint("SendMsg error: ", err)
    }
    return err
}

// Heartbeat

func (n node) Heartbeat() {
    msg := n.getAddrMsg()
    var reply ReplyMsg
    err := n.SendMsg("Heartbeat", &msg, &reply)
    if err != nil {
        n.logPrint("Heartbeat error: ", err)
    }
}

// Join Leave Repair

func (n *node) Join(addr string) error {
    var reply NodeMsg
    var err error
    var client *rpc.Client
    //n.Listen()

    for i := 0; i < retryCount; i++ {
        client, err = rpc.DialHTTP("tcp", addr)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        n.logPrint("_join dial error: ", err)
        // multiple reply delays ?
        time.Sleep(retryDelay)

    }
    if err != nil {
        return err
    }

    for i := 0; i < retryCount; i++ {
        err = client.Call("NodeRpc.Join", n.getAddrMsg(), &reply)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        n.logPrint("_join call error: ", err)
        // multiple reply delays ?
        time.Sleep(retryDelay)

    }
    if err != nil {
        return err
    }

    n.logicalClock.SetIfHigherAndInc(reply.GetLogicalTime())
    n.leaderUid = reply.Uid

    if reply.Addr == "" {
        log.Fatal("Got empty addr as reply w/o error.")
    }
    if reply.Addr == n.addr {
        // do not accept self as neighbour
        n.logPrint("Joined broken ring - expecting repair")
        return nil
    }

    err = n.DialNeighbour(reply.Addr)
    if err != nil {
        return err
    }
    n.leftTheCluster = false

    go n.HeartbeatChecker()
    go n.HeartbeatSender()

    n.logPrint("Joined")
    n.Read()
    return err
}

func (n *node) LeaveWithoutMsg() {
    n.leftTheCluster = true
    n.neighbourAddr = ""
    n.neighbourRpc = nil
    n.leaderUid = 0
    n.participatingInElection = false
    n.logPrint("Left w/o message")
}
func (n *node) Leave() error {
    var reply ReplyMsg
    msg := TwoAddrMsg {n.getAddrMsg(), n.neighbourAddr}
    n.logPrint("Sending Leave ", msg)
    err := n.SendMsg("Leave", &msg, &reply)
    if err != nil {
        n.logPrint("Leave error:", err)
        return err
    }

    n.LockMtx()
    defer n.UnlockMtx()
    n.neighbourAddr = ""
    n.neighbourRpc = nil

    n.leftTheCluster = true
    n.neighbourAddr = ""
    n.neighbourRpc = nil
    n.leaderUid = 0
    n.participatingInElection = false
    n.logPrint("Left")
    return nil
}

func (n *node) FwdLeave(msg *TwoAddrMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding Leave", *msg)
    return n.SendMsg("Leave", msg, reply)
}

func (n *node) Repair() error {
    var reply ReplyMsg
    msg := n.getAddrMsg()
    n.logPrint("Sending Repair ", msg)
    err := n.SendMsg("Repair", &msg, &reply)
    if err != nil {
        n.logPrint("Repair error:", err)
        return err
    }
    return err
}

func (n *node) FwdRepair(msg *AddrMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding Repair", *msg)
    return n.SendMsg("Repair", msg, reply)
}

// Vote ElectedMsg

func (n *node) Vote() error {
    var reply ReplyMsg
    msg := n.getUidMsg()
    n.logPrint("Sending Vote ", msg)
    err := n.SendMsg("Vote", &msg, &reply)
    if err != nil {
        n.logPrint("Vote error:", err)
        return err
    }
    n.participatingInElection = true
    return err
}

func (n *node) FwdVote(msg *UidMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding Vote ", *msg)
    err := n.SendMsg("Vote", msg, reply)
    if err == nil {
        n.participatingInElection = true
    }
    return err
}

func (n *node) ElectedMsg() error {
    var reply ReplyMsg
    msg := UidMsg {n.getBaseMsg(), n.uid}
    n.logPrint("Sending ElectedMsg ", msg)

    err := n.SendMsg("ElectedMsg", &msg, &reply)
    if err != nil {
        n.logPrint("ElectedMsg error:", err)
        return err
    }
    n.participatingInElection = false
    return err
}

func (n *node) FwdElectedMsg(msg *UidMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding ElectedMsg ", *msg)

    err := n.SendMsg("ElectedMsg", msg, reply)
    if err == nil {
        n.participatingInElection = true
    }
    return err
}


// Read Write Broadcast

func (n *node) Read() (int, error) {
    if n.leaderUid == n.uid {
        return n.sharedVariable, nil
    }
    var reply VarMsg
    msg := n.getUidMsg()
    n.logPrint("Sending Read", msg)
    err := n.SendMsg("Read", &msg, &reply)
    if err != nil {
        n.logPrint("Read error: ", err)
    }
    return reply.SharedVariable, err
}

func (n *node) FwdRead(msg *UidMsg, reply *VarMsg) (int, error) {
    n.logPrint("Forwarding read", *msg)
    err := n.SendMsg("Read", msg, reply)
    if err == nil {
        n.sharedVariable = reply.SharedVariable
    }
    return reply.SharedVariable, err
}

func (n *node) Write(value int) error {
    if n.leaderUid == n.uid {
        n.sharedVariable = value
        n.Broadcast()
        return nil
    }
    var reply ReplyMsg
    msg := VarMsg {n.getNodeMsg(), value}
    n.logPrint("Sending Write", msg)
    err := n.SendMsg("Write", &msg, &reply)
    if err != nil {
        n.logPrint("Write error: ", err)
        return err
    }
    return err
}

func (n node) FwdWrite(msg *VarMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding Write", *msg)
    return n.SendMsg("Write", msg, reply)
}

func (n node) Broadcast() {
    var reply ReplyMsg
    msg := VarMsg {n.getNodeMsg(), n.sharedVariable}
    n.logPrint("Sending Broadcast", msg)
    err := n.SendMsg("Broadcast", &msg, &reply)
    if err != nil {
        n.logPrint("Broadcast error: ", err)
    }
}

func (n node) FwdBroadcast(msg *VarMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding Broadcast ", *msg)
    return n.SendMsg("Broadcast", msg, reply)
}

// more functions

func (n *node) Listen() error {
    //if n.server != nil {
    //    return errors.New("Node: Already listenning")
    //}
    r := &NodeRpc {
        Node: n,
    }
    rpc.Register(r)
    rpc.HandleHTTP()
    port := ":" + strings.Split(n.addr, ":")[1]
    l, err := net.Listen("tcp", port)
    if err != nil {
        n.logPrint("Listen error (never happens):", err)
        return err
    }
    n.server = new(http.Server)
	go n.server.Serve(l)
    n.logPrint("Listenning on", n.addr)
    return nil
}

func (n *node) HeartbeatChecker() {
    n.heartbeatTs = time.Now() // init
    for n.leftTheCluster == false {
        ts := n.heartbeatTs
        heartbeatExpiration := ts.Add(heartbeatTimeout)
        if time.Now().After(heartbeatExpiration) {
            n.logPrint("Heartbeat expired at", heartbeatExpiration)
            err := n.Repair()
            time.Sleep(heartbeatTimeout)
            if err != nil {
                //time.Sleep(heartbeatTimeout)
            }
            continue
        }
        sleepDuration := heartbeatExpiration.Sub(time.Now())
        //n.logPrint("Sleeping for", sleepDuration)
        time.Sleep(sleepDuration)
    }
}

func (n *node) HeartbeatSender() {
    for n.leftTheCluster == false {
        n.Heartbeat()
        time.Sleep(heartbeatInterval)
    }
}

func (n *node) Run() {
    for {
        for i := 0; i < 3; i++ {
            time.Sleep(3000 * time.Millisecond)
        }
        n.logPrint("ready.")
        n.PrintState()
        time.Sleep(3000 * time.Millisecond)
    }
}

func (n *node) RunLeave() {
    for {
        for j := 0; j < 10; j++ {
            for i := 0; i < 2; i++ {
                n.logPrint("ready.")
                n.Heartbeat()
                time.Sleep(5000 * time.Millisecond)
            }
            n.PrintState()
            time.Sleep(5000 * time.Millisecond)
            value, err := n.Read()
            if err != nil {
                n.logPrint("Read failed")
            } else {
                n.logPrint("Read: ", value)
            }
            if j == 3 {
                n.logPrint("Write: 111")
                n.Write(111)
            }
        }
        n.Leave()
        return
    }
}

