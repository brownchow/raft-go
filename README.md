# Go实现Raft分布式共识算法



## 使用

raft 网络中的节点使用 Server 结构体表示的，节点创建一个 Server，然后通过 peer 接口暴露给其他节点

node 之间的通信可以通过 http transport 和 http Peer。



## 增加和删除节点





## TODO

- Leader election
- Log Replication
- Basic Unit Test
- Http transport
- net/rpc transport
- Configuration changes (joint consensus mode)
- Log compaction
- Robost demo application
- complex unit test 



## reference

https://github.com/peterbourgon/raft

https://en.wikipedia.org/wiki/Raft_(algorithm)



## Usage

分布式存储

https://github.com/siddontang/rust-raftkv

