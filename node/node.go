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
const (
    // send Heartbeat every
    heartbeatInterval time.Duration = 5 * time.Second
    // send Reapir if you don't get Heartbeat for
    heartbeatTimeout time.Duration = 10 * time.Second
    // shutdown if you don't get Heartbeat for
    heartbeatTimeoutShutdown time.Duration = 25 * time.Second

    // how long to wait before retry
    retryDelay time.Duration = 100 * time.Millisecond
    // how long to wait before retry if we have no neighbour
    //      long time because we are waiting for repair (and heartbeatTimeout)
    retryDelayNoNeighbour time.Duration = 5000 * time.Millisecond
    // how long to wait before retry on failed join 
    retryDelayJoin time.Duration = 3000 * time.Millisecond

    // how many times to retry  
    retryCount int = 3
    // how many hops can message make before being discarded
    // prevents messages from staying in the ring forver 
    //      if recipient node leaves the ring
    // NOTE: ttl has to be higher than number of nodes
    initTtl int = 10

    errNoNeighbourRpc string = "No neighbour - neighbourRpc == nil"
    errNodeShutdown string = "Node is shut down"
    errNoLeaderStartElection string = "No leader - start election"
)

type Msg interface {
    GetLogicalTime() int64
    SetLogicalTime(int64)
    DecTtl() error
}

type ReplyMsg interface {
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

type TimeMsg struct {
    LogicalTime
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

func (r *NodeRpc) Heartbeat(msg AddrMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.SetHeartbeatTs(time.Now())
    r.Node.logPrint("Recieved Heartbeat from", msg.Addr)

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return nil
}

func (r *NodeRpc) Join(msg AddrMsg, reply *NodeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Join from", msg.Addr)

    err := func() error {
        r.Node.LockMtx()
        defer r.Node.UnlockMtx()
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

func (r *NodeRpc) Leave(msg TwoAddrMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Leave from", msg.Addr)
    var err error
    if msg.Addr != r.Node.neighbourAddr {
        err = r.Node.FwdLeave(&msg, reply)
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return err
    }

    _, err = r.Node.DialNeighbour(msg.NewAddr)
    if err != nil {
        r.Node.logPrint("NodeRpc: Leave request declined!")
        err = errors.New("NodeRpc: Leave request declined!")
    }

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Repair(msg AddrMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())

    r.Node.logPrint("Recieved Repair for", msg.Addr)
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

func (r *NodeRpc) Vote(msg UidMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Vote for", msg.Uid)

    var err error
    if msg.Uid > r.Node.uid {
        err = r.Node.FwdVote(&msg, reply)
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return err
    } else if msg.Uid < r.Node.uid {
        if r.Node.InElection() == false {
            msg.Uid = r.Node.uid
            msg.Ttl = initTtl // reset ttl
            err = r.Node.FwdVote(&msg, reply)
        }
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return err
    }
    r.Node.logPrint("Election done - I'm the leader")
    r.Node.logPrint("Starting post-election - I'm the leader")
    // elected
    r.Node.SetLeaderUid(r.Node.uid)
    r.Node.SetInElection(false)
    // post election
    err = r.Node.ElectedMsg()

    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) ElectedMsg(msg UidMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved ElectedMsg from", msg.Uid)
    r.Node.SetInElection(false)

    var err error
    if msg.Uid != r.Node.uid {
        r.Node.logPrint("ElectedMsg: Leader is", msg.Uid)
        err = r.Node.FwdElectedMsg(&msg, reply)
        r.Node.SetLeaderUid(msg.Uid)
    } else {
        r.Node.logPrint("Post-election done - I'm the leader")
        go r.Node.Broadcast()
        err = nil
    }
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Read(msg UidMsg, reply *VarMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Read from", msg.Uid)

    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recieved Read")
        // i'm the leader
        reply.SharedVariable = r.Node.SharedVariable()
        go r.Node.Broadcast()
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("Read: No leader - start election")
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return errors.New(errNoLeaderStartElection)
    }
    value, err := r.Node.FwdRead(&msg, reply)
    if err == nil {
        reply.SharedVariable = value
    }
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Write(msg VarMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Write", msg.SharedVariable,"from", msg.Uid)

    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recieved Write")
        // i'm the leader
        r.Node.sharedVariable = msg.SharedVariable
        go r.Node.Broadcast()
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("Write: No leader - start election")
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return errors.New(errNoLeaderStartElection)
    }
    err := r.Node.FwdWrite(&msg, reply)
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
}

func (r *NodeRpc) Broadcast(msg VarMsg, reply *TimeMsg) error {
    if r.Node.LeftTheCluster() {
        return errors.New(errNodeShutdown)
    }
    r.Node.logicalClock.SetIfHigherAndInc(msg.GetLogicalTime())
    r.Node.logPrint("Recieved Broadcast", msg.SharedVariable, "from", msg.Uid)

    if r.Node.leaderUid == r.Node.uid {
        r.Node.logPrint("Leader recieved Broadcast")
        // i'm the leader
        // end Broadcast
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    if msg.Uid == r.Node.uid {
        r.Node.logPrint("Broadcast ended at original node")
        // full roundtrip
        // end Broadcast 
        reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
        return nil
    }
    // set sharedVariable
    r.Node.sharedVariable = msg.SharedVariable
    err := r.Node.FwdBroadcast(&msg, reply)
    reply.SetLogicalTime(r.Node.logicalClock.GetAndInc())
    return err
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
    for _, ipPart := range strings.Split(ip.String(), ".") {
        ipPartInt, _ := strconv.Atoi(ipPart)
        uid += int64(ipPartInt)
        uid *= 1000
    }
    uid *= 1000
    uid += int64(port)
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
    fmt.Println("uid:", n.uid)
    fmt.Println("addr:", n.addr)
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


func (n *node) _sendMsg(method string, msg Msg, reply ReplyMsg) error {
    n.RLockMtx()
    defer n.RUnlockMtx()
    if n.neighbourRpc == nil {
        // n.logPrint("No neighbourRpc")
        return errors.New(errNoNeighbourRpc)
    }
    msg.SetLogicalTime(n.logicalClock.GetAndInc())

    err := n.neighbourRpc.Call(method, msg, reply)
    if err == nil {
        n.logicalClock.SetIfHigherAndInc(reply.GetLogicalTime())
    }
    return err
}

func (n *node) SendMsg(method string, msg Msg, reply ReplyMsg) error {
    var err error
    serviceMethod := "NodeRpc." + method
    err = msg.DecTtl()
    if err != nil {
        // this will probably go away or become debug only
        //n.logPrint("SendMsg error:", err)
        return err
    }

    for i := 0; i < retryCount; i++ {
        err = n._sendMsg(serviceMethod, msg, reply)
        if err == nil {
            break
        }
        // this will probably go away or become debug only
        //n.logPrint("_sendMsg error:", err)

        switch err.Error() {
        case errNodeShutdown:
            return err
        case errNoLeaderStartElection:
            return err
        case errNoNeighbourRpc:
            return err
        default:
            //n.logPrint("SendMsg error:", err)
            time.Sleep(retryDelay)
        }
    }
    // this will probably go away or become debug only
    //if err != nil {
    //    n.logPrint("SendMsg error:", err)
    //}
    return err
}

// Heartbeat

func (n *node) Heartbeat() {
    msg := n.getAddrMsg()
    var reply TimeMsg
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
    var reply TimeMsg

    n.RLockMtx()
    msg := TwoAddrMsg {n.getAddrMsg(), n.neighbourAddr}
    n.RUnlockMtx()

    n.logPrint("Sending Leave, neighbour:", msg.NewAddr)
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

func (n *node) FwdLeave(msg *TwoAddrMsg, reply *TimeMsg) error {
    n.logPrint("Forwarding Leave", *msg)
    return n.SendMsg("Leave", msg, reply)
}

func (n *node) Repair() error {
    var reply TimeMsg
    msg := n.getAddrMsg()
    n.logPrint("Sending Repair")
    err := n.SendMsg("Repair", &msg, &reply)
    if err != nil {
        n.logPrint("Repair error:", err)
        return err
    }
    return err
}

func (n *node) FwdRepair(msg *AddrMsg, reply *TimeMsg) error {
    n.logPrint("Forwarding Repair", *msg)
    return n.SendMsg("Repair", msg, reply)
}

// Vote ElectedMsg

func (n *node) Vote() error {
    var reply TimeMsg
    msg := n.getUidMsg()
    n.logPrint("Sending Vote for", msg.Uid)
    n.SetInElection(true)
    err := n.SendMsg("Vote", &msg, &reply)
    if err != nil {
        n.logPrint("Vote error:", err)
        return err
    }
    //n.SetInElection(false)
    return err
}

func (n *node) FwdVote(msg *UidMsg, reply *TimeMsg) error {
    n.logPrint("Forwarding Vote", *msg)
    n.SetInElection(true)
    err := n.SendMsg("Vote", msg, reply)
    return err
}

func (n *node) ElectedMsg() error {
    var reply TimeMsg
    msg := UidMsg {n.getBaseMsg(), n.uid}
    n.logPrint("Sending ElectedMsg, uid:", msg.Uid)

    n.SetInElection(false)
    err := n.SendMsg("ElectedMsg", &msg, &reply)
    if err != nil {
        n.logPrint("ElectedMsg error:", err)
        return err
    }
    return err
}

func (n *node) FwdElectedMsg(msg *UidMsg, reply *TimeMsg) error {
    n.logPrint("Forwarding ElectedMsg", *msg)

    err := n.SendMsg("ElectedMsg", msg, reply)
    return err
}


// Read Write Broadcast

func (n *node) Read() (int, error) {
    if n.LeaderUid() == n.uid {
        return int(n.SharedVariable()), nil
    }
    var reply VarMsg
    msg := n.getUidMsg()
    n.logPrint("Sending Read")
    err := n.SendMsg("Read", &msg, &reply)
    if err != nil {
        n.logPrint("Read error: ", err)
        if err.Error() == errNoLeaderStartElection {
            err = n.Vote()
            if err != nil {
                n.logPrint("Vote failed", err)
                return int(reply.SharedVariable), err
            }
            n.logPrint("Sending Read again", msg)
            msg = n.getUidMsg()
            err = n.SendMsg("Read", &msg, &reply)
            if err != nil {
                n.logPrint("Read again failed", err)
            }
        }
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
    var reply TimeMsg
    msg := VarMsg {n.getNodeMsg(), int32(value)}
    n.logPrint("Sending Write", msg.SharedVariable)
    err := n.SendMsg("Write", &msg, &reply)
    if err != nil {
        n.logPrint("Write error: ", err)
        if err.Error() == errNoLeaderStartElection {
            err = n.Vote()
            if err != nil {
                n.logPrint("Vote failed", err)
                return err
            }
            n.logPrint("Sending Write again", msg)
            msg = VarMsg {n.getNodeMsg(), int32(value)}
            err = n.SendMsg("Write", &msg, &reply)
            if err != nil {
                n.logPrint("Write again failed", err)
            }
        }
    }
    return err
}

func (n *node) FwdWrite(msg *VarMsg, reply *TimeMsg) error {
    n.logPrint("Forwarding Write", *msg)
    return n.SendMsg("Write", msg, reply)
}

func (n *node) Broadcast() {
    var reply TimeMsg
    msg := VarMsg {n.getNodeMsg(), n.sharedVariable}
    n.logPrint("Sending Broadcast", msg.SharedVariable)
    err := n.SendMsg("Broadcast", &msg, &reply)
    if err != nil {
        n.logPrint("Broadcast error: ", err)
    }
}

func (n *node) FwdBroadcast(msg *VarMsg, reply *TimeMsg) error {
    n.logPrint("Forwarding Broadcast", *msg)
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

