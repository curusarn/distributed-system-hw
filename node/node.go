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
    "sync/atomic"
    lampartsclock "github.com/curusarn/distributed-system-hw/lampartsclock"
)

// send Heartbeat every
const heartbeatInterval time.Duration = 5 * time.Second
// send Reapir if you don't get Heartbeat for
const heartbeatTimeout time.Duration = 10 * time.Second
// shutdown if you don't get Heartbeat for
const heartbeatTimeoutShutdown time.Duration = 25 * time.Second

// how long to wait before retry
const retryDelay time.Duration = 100 * time.Millisecond
// how long to wait before retry if msg started election
const retryDelayElection time.Duration = 3000 * time.Millisecond
// how long to wait before retry if we have no neighbour
//      long time because we are waiting for repair (and heartbeatTimeout)
const retryDelayNoNeighbour time.Duration = 5000 * time.Millisecond
// how long to wait before retry on failed join 
const retryDelayJoin time.Duration = 3000 * time.Millisecond

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
    SharedVariable int32
}

// NodeRpc
// RPC wrapper for node struct
type NodeRpc struct {
    Node *node
}

func (r *NodeRpc) Heartbeat(msg AddrMsg, reply *ReplyMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.SetHeartbeatTs(time.Now())
    r.Node.logPrint("Recieved Heartbeat from", msg.Addr)

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Join(msg AddrMsg, reply *NodeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Join", msg.Addr)

    err := func() error {
        r.Node.LockMtx()
        if r.Node.neighbourAddr == "" {
            return errors.New("NodeRpc: Join request declined!")
        }
        return nil
    }()
    if err != nil {
        return err
    }
    reply.Uid = r.Node.LeaderUid() // pass leader uid to joining node

    reply.Addr, err = r.Node.DialNeighbour(msg.Addr)
    if err != nil {
        r.Node.logPrint("NodeRpc: Join request declined!")
        err = errors.New("NodeRpc: Join request declined!")
        reply.Addr = ""
    }

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Leave(msg TwoAddrMsg, reply *ReplyMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Leave", msg)
    // what if I do not have a neighbour ?
    if msg.Addr != r.Node.neighbourAddr {
        r.Node.FwdLeave(&msg, reply)
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }

    _, err := r.Node.DialNeighbour(msg.NewAddr)
    if err != nil {
        r.Node.logPrint("NodeRpc: Leave request declined!")
        err = errors.New("NodeRpc: Leave request declined!")
    }

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Repair(msg AddrMsg, reply *ReplyMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Repair", msg)
    r.Node.logPrint("My neighbour is", r.Node.neighbourAddr)

    err := r.Node.FwdRepair(&msg, reply)
    if err != nil {
        err = nil
        r.Node.logPrint("Reached loose end of ring")
        // failed FwdRepair -> we are the last node
        oldAddr, err := r.Node.DialNeighbour(msg.Addr)
        if err != nil {
            err = errors.New("NodeRpc: Repair request declined!")
        } else {
            r.Node.logPrint("Topology Repaired, lost node", oldAddr)
        }
    }
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Vote(msg UidMsg, reply *ReplyMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Vote", msg)

    if msg.Uid > r.Node.uid {
        r.Node.FwdVote(&msg, reply)
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    } else if msg.Uid < r.Node.uid {
        if r.Node.InElection() == false {
            msg.Uid = r.Node.uid
            msg.Ttl = initTtl // reset ttl
            r.Node.FwdVote(&msg, reply)
        }
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    r.Node.logPrint("Election done - I'm the leader")
    r.Node.logPrint("Starting post-election - I'm the leader")
    // elected
    r.Node.SetLeaderUid(r.Node.uid)
    r.Node.SetInElection(false)
    // post election
    r.Node.ElectedMsg()

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) ElectedMsg(msg UidMsg, reply *ReplyMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved ElectedMsg", msg)

    if msg.Uid != r.Node.uid {
        r.Node.FwdElectedMsg(&msg, reply)
        r.Node.SetLeaderUid(msg.Uid)
        r.Node.SetInElection(false)
    } else {
        r.Node.logPrint("Post-election done - I'm the leader")
        r.Node.Broadcast()
    }
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Read(msg UidMsg, reply *VarMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Read", msg)

    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recieved Read")
        // i'm the leader
        reply.SharedVariable = r.Node.SharedVariable()
        r.Node.Broadcast()
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.Vote() // this is blocking - we might just repeat the read
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return errors.New("Read: No leader - starting election")
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
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
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
    if r.Node.LeftTheCluster() {
        return errors.New("Node: shutdown")
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Broadcast", msg)

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

    // mtx protected access
    neighbourAddr string
    neighbourRpc *rpc.Client
    neighbourMtx *sync.RWMutex

    leaderUid int64
    // bool -> int32 (because of atomic)
    inElection int32

    // time.Time -> int64 (because of atomic)
    heartbeatTs int64

    // int -> int32 (because of atomic)
    sharedVariable int32

    // has thread-safe methods
    logicalClock *lampartsclock.LampartsClock

    // logger has inplicit mtx 
    logger *log.Logger

    // bool -> int32 (because of atomic)
    leftTheCluster int32
}

// thread-safe getters and setters
func (n *node) LeaderUid() int64 {
    return atomic.LoadInt64(&n.leaderUid)
}

func (n *node) SetLeaderUid(value int64) {
    atomic.StoreInt64(&n.leaderUid, value)
}

func (n *node) SharedVariable() int32 {
    return atomic.LoadInt32(&n.sharedVariable)
}

func (n *node) SetSharedVariable(value int32) {
    atomic.StoreInt32(&n.sharedVariable, value)
}

func (n *node) HeartbeatTs() time.Time {
    return time.Unix(0, atomic.LoadInt64(&n.heartbeatTs))
}

func (n *node) SetHeartbeatTs(value time.Time) {
    atomic.StoreInt64(&n.heartbeatTs, value.UnixNano())
}

func (n *node) InElection() bool {
    return (atomic.LoadInt32(&n.inElection) != 0)
}

func (n *node) SetInElection(value bool) {
    if value {
        atomic.StoreInt32(&n.inElection, 1)
    } else {
        atomic.StoreInt32(&n.inElection, 0)
    }
}

func (n *node) LeftTheCluster() bool {
    return (atomic.LoadInt32(&n.leftTheCluster) != 0)
}

func (n *node) SetLeftTheCluster(value bool) {
    if value {
        atomic.StoreInt32(&n.leftTheCluster, 1)
    } else {
        atomic.StoreInt32(&n.leftTheCluster, 0)
    }
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

func (n *node) PrintState() {
    fmt.Println("inElection:", n.InElection())
    fmt.Println("leftCluster:", n.LeftTheCluster())
    fmt.Println("shraredVar:", n.SharedVariable())
    fmt.Println("leader:", n.LeaderUid())
    n.RLockMtx()
    defer n.RUnlockMtx()
    fmt.Println("neighbour:", n.neighbourAddr)
}

func (n *node) InitCluster() error {
    //n.Listen()
    n.SetSharedVariable(-1) // init sharedVariable to -1 
    n.SetLeaderUid(n.uid) // set yourself as leader
    _, err := n.DialNeighbour(n.addr) // set yourself as neighbour and dial
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

func (n *node) DialNeighbour(newAddr string) (string, error) {
    n.LockMtx()
    defer n.UnlockMtx()
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
        return "", err
    }

    if n.neighbourRpc != nil {
        n.neighbourRpc.Close()
        n.neighbourRpc = nil
    }
    prev := n.neighbourAddr
    n.neighbourAddr = newAddr
    n.neighbourRpc = client
    n.logPrint("Dialing succes")
    return prev, nil
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
        n.logPrint("SendMsg error:", err)
        return err
    }

    for i := 0; i < retryCount; i++ {
        err = n._sendMsg(serviceMethod, msg, reply)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        n.logPrint("_sendMsg error:", err)
        // multiple reply delays based on error
        if strings.Contains(err.Error(), "Node: shutdown") {
            break
        } else if strings.Contains(err.Error(), "starting election") {
            time.Sleep(retryDelayElection)
        } else if strings.Contains(err.Error(), "No neighbour: neighbourRpc == nil") {
            break
        } else {
            time.Sleep(retryDelay)
        }
    }
    if err != nil {
        // this will probably go away or become debug only
        n.logPrint("SendMsg error:", err)
    }
    return err
}

// Heartbeat

func (n node) Heartbeat() {
    msg := n.getAddrMsg()
    var reply ReplyMsg
    err := n.SendMsg("Heartbeat", &msg, &reply)
    if err != nil {
        n.logPrint("Heartbeat error:", err)
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
        n.logPrint("_join dial error:", err)
        time.Sleep(retryDelayJoin)

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
        n.logPrint("_join call error:", err)
        time.Sleep(retryDelayJoin)

    }
    if err != nil {
        return err
    }

    n.logicalClock.SetIfHigherAndInc(reply.GetLogicalTime())
    n.SetLeaderUid(reply.Uid)

    if reply.Addr == "" {
        n.logPrint("Fatal: Got empty addr as reply w/o error.")
    }
    if reply.Addr == n.addr {
        // do not accept self as neighbour
        n.logPrint("Joined broken ring - expecting repair")
        n.SetLeftTheCluster(false)
        return nil
    }

    _, err = n.DialNeighbour(reply.Addr)
    if err != nil {
        return err
    }
    n.SetLeftTheCluster(false)

    go n.HeartbeatChecker()
    go n.HeartbeatSender()

    n.logPrint("Joined")
    n.Read()
    return err
}

func (n *node) LeaveWithoutMsg() {
    n.SetLeftTheCluster(true)
    n.SetLeaderUid(0)
    n.SetInElection(false)

    n.LockMtx()
    defer n.UnlockMtx()
    n.neighbourAddr = ""
    n.neighbourRpc = nil
    n.logPrint("Left w/o message")
}
func (n *node) Leave() error {
    var reply ReplyMsg

    n.RLockMtx()
    msg := TwoAddrMsg {n.getAddrMsg(), n.neighbourAddr}
    n.RUnlockMtx()

    n.logPrint("Sending Leave ", msg)
    err := n.SendMsg("Leave", &msg, &reply)
    if err != nil {
        n.logPrint("Leave error:", err)
        return err
    }

    n.SetLeftTheCluster(true)
    n.SetLeaderUid(0)
    n.SetInElection(false)

    n.LockMtx()
    defer n.UnlockMtx()
    n.neighbourAddr = ""
    n.neighbourRpc = nil
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
    if n.InElection() {
        return errors.New("Node: can't Vote while InElection")
    }
    var reply ReplyMsg
    msg := n.getUidMsg()
    n.logPrint("Sending Vote ", msg)
    err := n.SendMsg("Vote", &msg, &reply)
    if err != nil {
        n.logPrint("Vote error:", err)
        return err
    }
    n.SetInElection(true)
    return err
}

func (n *node) FwdVote(msg *UidMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding Vote ", *msg)
    err := n.SendMsg("Vote", msg, reply)
    if err == nil {
        n.SetInElection(true)
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
    n.SetInElection(false)
    return err
}

func (n *node) FwdElectedMsg(msg *UidMsg, reply *ReplyMsg) error {
    n.logPrint("Forwarding ElectedMsg ", *msg)

    err := n.SendMsg("ElectedMsg", msg, reply)
    if err == nil {
        n.SetInElection(false)
    }
    return err
}


// Read Write Broadcast

func (n *node) Read() (int, error) {
    if n.LeaderUid() == n.uid {
        return int(n.SharedVariable()), nil
    }
    var reply VarMsg
    msg := n.getUidMsg()
    n.logPrint("Sending Read", msg)
    err := n.SendMsg("Read", &msg, &reply)
    if err != nil {
        n.logPrint("Read error: ", err)
    }
    return int(reply.SharedVariable), err
}

func (n *node) FwdRead(msg *UidMsg, reply *VarMsg) (int32, error) {
    n.logPrint("Forwarding read", *msg)
    err := n.SendMsg("Read", msg, reply)
    if err == nil {
        n.sharedVariable = reply.SharedVariable
    }
    return reply.SharedVariable, err
}

func (n *node) Write(value int) error {
    if n.LeaderUid() == n.uid {
        n.SetSharedVariable(int32(value))
        n.Broadcast()
        return nil
    }
    var reply ReplyMsg
    msg := VarMsg {n.getNodeMsg(), int32(value)}
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
    srv := new(http.Server)
	go srv.Serve(l)
    n.logPrint("Listenning on", n.addr)
    return nil
}

func (n *node) HeartbeatChecker() {
    n.SetHeartbeatTs(time.Now()) // init
    for n.LeftTheCluster() == false {
        ts := n.HeartbeatTs()
        heartbeatExpiration := ts.Add(heartbeatTimeout)
        heartbeatExpirationShutdown := ts.Add(heartbeatTimeoutShutdown)
        if time.Now().After(heartbeatExpirationShutdown) {
            n.logPrint("Heartbeat expired at", heartbeatExpiration)
            n.logPrint("It's been too long - node is shuting down!")
            err := n.Leave()
            if err != nil {
                n.LeaveWithoutMsg()
            }
            continue
        }
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
    for n.LeftTheCluster() == false {
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

