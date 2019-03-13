# distributed-system-hw
This is a semestral work for MI-DSV course at CTU FIT

It is a simple distributed system that mainatains a shared variable.

- Uses leader election 
  - Chang-Roberts election algorithm
- Uses unidirectional ring topology
  - Each node knows about one neighbour
  - Nodes can join and leave the ring
  - Topology is resistent to one simultaneous node fail
  - Heartbeat messages are used to detect failed nodes 
- Uses Lamport's clock
  - Log messages have logical clock timestamp
- Written in Golang
- Uses GoRPC for message sending

Run 5 node cluster using Vagrant like this:  
1) `vagrant up` - creates 5 virtual machines
2) Run on each machine: `make dsvX` (replace X with machine number)
