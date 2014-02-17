package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"time"
)

// Btree metadata
type Btree struct {
	BtreeMetaData
	nodes       []TreeNode
	dupnodelist []int64
	opChan      chan *treeOperation
	exitChan    chan int
}

// Node is btree node
type Node struct {
	NodeRecordMetaData
}

// Leaf is btree leaf
type Leaf struct {
	LeafRecordMetaData
}

// Record is data record, which will be stored in btree
type Record struct {
	Key   []byte
	Value []byte
}

// TreeSize is  tree size
const TreeSize = 1 << 10

// LeafSize is leaf size
const LeafSize = 1 << 5

// NodeSize is node size
const NodeSize = 1 << 6

// isNode, isLeaf is treenode tag
const (
	isNode = iota
	isLeaf
)

// NewRecord create record
func NewRecord(key, value []byte) *Record {
	return &Record{key, value}
}

// NewBtree create a btree
func NewBtree() *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, TreeSize)
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int64(TreeSize),
		LeafMax:     proto.Int64(LeafSize),
		NodeMax:     proto.Int64(NodeSize),
		LeafCount:   proto.Int64(0),
		NodeCount:   proto.Int64(0),
		IndexCursor: proto.Int64(0),
	}
	leaf := tree.newLeaf()
	tree.Root = proto.Int64(leaf.GetId())
	tree.exitChan = make(chan int)
	tree.opChan = make(chan *treeOperation)
	go tree.run()
	return tree
}

// NewBtreeSize create new btree with custom leafsize/nodesize
func NewBtreeSize(leafsize int64, nodesize int64) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, TreeSize)
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int64(TreeSize),
		LeafMax:     proto.Int64(leafsize),
		NodeMax:     proto.Int64(nodesize),
		LeafCount:   proto.Int64(0),
		NodeCount:   proto.Int64(0),
		IndexCursor: proto.Int64(0),
	}
	leaf := tree.newLeaf()
	tree.Root = proto.Int64(leaf.GetId())
	tree.exitChan = make(chan int)
	tree.opChan = make(chan *treeOperation)
	go tree.run()
	return tree
}

func (t *Btree) run() {
	tick := time.Tick(time.Second * 10)
	for {
		select {
		case <-t.exitChan:
			break
		case op := <-t.opChan:
			switch op.Action {
			case "insert":
				op.restChan <- t.insert(op.Payload.(*Record))
			case "delete":
				op.restChan <- t.dodelete(op.Payload.([]byte))
			case "update":
				op.restChan <- t.update(op.Payload.(*Record))
			case "search":
				op.restChan <- t.search(op.Payload.([]byte))
			}
		case <-tick:
			t.gc()
		}
	}
	t.Marshal("treedump.tmp")
}

func (t *Btree) Save(file string) {
	t.Marshal(file)
	close(t.exitChan)
}

// Insert can insert record into a btree
func (t *Btree) Insert(record *Record) bool {
	q := &treeOperation{
		Action:   "insert",
		Payload:  record,
		restChan: make(chan interface{}),
	}
	t.opChan <- q
	rst := <-q.restChan
	return rst.(bool)
}

// Delete can delete record
func (t *Btree) Delete(key []byte) bool {
	q := &treeOperation{
		Action:   "delete",
		Payload:  key,
		restChan: make(chan interface{}),
	}
	t.opChan <- q
	rst := <-q.restChan
	return rst.(bool)
}

// Search return value
func (t *Btree) Search(key []byte) []byte {
	q := &treeOperation{
		Action:   "search",
		Payload:  key,
		restChan: make(chan interface{}),
	}
	t.opChan <- q
	rst := <-q.restChan
	return rst.([]byte)
}

// Update is used to update key/value
func (t *Btree) Update(record *Record) bool {
	q := &treeOperation{
		Action:   "update",
		Payload:  record,
		restChan: make(chan interface{}),
	}
	t.opChan <- q
	rst := <-q.restChan
	return rst.(bool)
}
