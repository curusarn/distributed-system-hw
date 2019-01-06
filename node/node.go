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
        r.Node.Vote(uid)
        return nil
    } else if uid < r.Node.uid {
        if r.Node.participatingInElection == false {
            r.Node.Vote(r.Node.uid)
        }
        return nil
    }
    // elected
    r.Node.leaderUid = r.Node.uid
    r.Node.participatingInElection = false
    // post election
    r.Node.ElectedMsg(r.Node.uid)
    return nil
}

func (r *NodeRpc) ElectedMsg(uid int64, reply *bool) error {
    if uid != r.Node.uid {
        r.Node.ElectedMsg(uid)
        r.Node.leaderUid = uid
        r.Node.participatingInElection = false
    }
    return nil
}

func (r *NodeRpc) Read(info NodeInfo, variable *int) error {
    log.Print("Recieved Read ", info)
    if info.Uid == r.Node.uid {
        log.Print("No leader - starting election")
        // full roundtrip
        // start election by voting for self
        r.Node.Vote(r.Node.uid)
        return errors.New("No leader - starting election")
    }
    if r.Node.leaderUid == r.Node.uid {
        log.Print("Leader recv Read")
        // i'm the leader
        *variable = r.Node.sharedVariable
        return nil
    }
    *variable = r.Node.Read(info)
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

func (n *node) Join(addr string) {
    // dial before call ... duh
    client, err := rpc.DialHTTP("tcp", addr)
    if err != nil {
        log.Fatal("dialing:", err)
        // retry?
        // then Fatal
    }

    var reply NodeInfo
    log.Print("Join ...")
    err = client.Call("NodeRpc.Join", n.getNodeInfo(), &reply)
    if err != nil {
        log.Fatal("call error:", err)
    }
    log.Print("join atempt")

    if reply.Addr == n.addr {
        // do not accept self as neighbour
        log.Print("joined broken ring - expecting repair")
        return
    }

    n.LockMtx()
    n.neighbourAddr = reply.Addr
    n.DialNeighbour()
    n.UnlockMtx()

    n.leaderUid = reply.Uid

    log.Print("joined")
}

func (n node) Leave() {
    // Synchronous call
    n.RLockMtx()
    info := TwoNodeInfo {n.getNodeInfo(), 0, n.neighbourAddr}
    var reply bool
    log.Print("Leave ...")

    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        //return errors.New("No neighbour: neighbourRpc == nil")
        n.RUnlockMtx()
        return
    }

    err := n.neighbourRpc.Call("NodeRpc.Leave", info, &reply)
    n.RUnlockMtx()

    if err != nil {
        log.Print("call error:", err)
        time.Sleep(10 * time.Millisecond)

        log.Fatal("call error:", err)
    }

    n.LockMtx()
    n.neighbourAddr = ""
    n.neighbourRpc = nil
    n.UnlockMtx()

    log.Print("Left")
}

func (n node) Repair() {
    // Synchronous call
    var reply bool
    log.Print("Repair ...")

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        //return errors.New("No neighbour: neighbourRpc == nil")
        n.RUnlockMtx()
        return
    }
    err := n.neighbourRpc.Call("NodeRpc.Repair", n.getNodeInfo(), &reply)
    n.RUnlockMtx()
    if err != nil {
        log.Fatal("call error:", err)
    }
    log.Print("Repair done")
}

func (n node) FwdLeave(info TwoNodeInfo) {
    // Synchronous call
    var reply bool
    log.Print("FwdLeave ...")

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        //return errors.New("No neighbour: neighbourRpc == nil")
        n.RUnlockMtx()
        return
    }
    err := n.neighbourRpc.Call("NodeRpc.Leave", info, &reply)
    n.RUnlockMtx()
    if err != nil {
        log.Fatal("call error:", err)
    }
    log.Print("FwdLeave done")
}

func (n node) FwdRepair(info NodeInfo) error {
    // Synchronous call
    var reply bool
    log.Print("FwdRepair ...")

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        return errors.New("No neighbour: neighbourRpc == nil")
    }
    err := n.neighbourRpc.Call("NodeRpc.Repair", info, &reply)
    n.RUnlockMtx()
    if err != nil {
        log.Print("call error:", err)
        return err
    }
    log.Print("FwdRepair done")
    return nil
}

func (n node) Vote(uid int64) error {
    // Synchronous call
    var reply bool
    log.Print("Vote ... for ", uid)

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        return errors.New("No neighbour: neighbourRpc == nil")
    }
    err := n.neighbourRpc.Call("NodeRpc.Vote", uid, &reply)
    n.RUnlockMtx()
    if err != nil {
        log.Print("call error:", err)
        return err
    }
    n.participatingInElection = true
    log.Print("Vote done")
    return nil
}

func (n node) ElectedMsg(uid int64) error {
    // Synchronous call
    var reply bool
    log.Print("ElectedMsg ... ", uid)

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        return errors.New("No neighbour: neighbourRpc == nil")
    }
    err := n.neighbourRpc.Call("NodeRpc.ElectedMsg", uid, &reply)
    n.RUnlockMtx()
    if err != nil {
        log.Print("call error:", err)
        return err
    }
    log.Print("ElectedMsg done")
    return nil
}

func (n node) Read(info NodeInfo) int {
    // Synchronous call
    var sharedVariable int
    log.Print("Read ... ", info)

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        // TODO return error and sharedVariable
        log.Fatal("No neighbourRpc")
        //return errors.New("No neighbour: neighbourRpc == nil")
    }
    err := n.neighbourRpc.Call("NodeRpc.Read", info, &sharedVariable)
    n.RUnlockMtx()
    if err != nil {
        log.Print("call error:", err)

        // TODO return error and sharedVariable
        //return err
    }
    n.sharedVariable = sharedVariable
    log.Print("Read done")
    return sharedVariable
}

func (n *node) Run() {
    for {
        for i := 0; i < 3; i++ {
            log.Print("ready.")
            n.SendHeartbeat()
            time.Sleep(5000 * time.Millisecond)
        }
        n.PrintState()
        time.Sleep(5000 * time.Millisecond)
    }
}

func (n *node) RunLeave() {
    for {
        for i := 0; i < 10; i++ {
            for i := 0; i < 2; i++ {
                log.Print("ready.")
                n.SendHeartbeat()
                time.Sleep(5000 * time.Millisecond)
            }
            n.PrintState()
            time.Sleep(5000 * time.Millisecond)
            log.Print("Read: ", n.Read(n.getNodeInfo()))
        }
        n.Leave()
        return
    }
}

func (n node) _sendHeartbeat() error {
    reply := false

    n.RLockMtx()
    if n.neighbourRpc == nil {
        log.Print("No neighbourRpc")
        n.RUnlockMtx()
        return errors.New("No neighbour: neighbourRpc == nil")
    }

    err := n.neighbourRpc.Call("NodeRpc.Heartbeat", true, &reply)
    n.RUnlockMtx()
    if err != nil {
        return err
    }
    return nil
}

func (n node) SendHeartbeat() {
    var err error
    for i := 0; i < retryCount; i++ {
        err = n._sendHeartbeat()
        if err == nil {
            break
        }
        log.Print("_sendHb error: ", err)
        time.Sleep(retryDelay)
    }
    if err != nil {
        log.Print("SendHb error: ", err)
    }
}

func (n *node) HeartbeatChecker() {
    n.heartbeatTs = time.Now() // init
    for {
        time.Sleep(heartbeatTimeout)
        ts := n.heartbeatTs
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
    fmt.Println("no hb -> RepairTopology ...")
    n.Repair()
}

func (n node) SendMsg(msg string) bool {
    return true
}
