package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"time"
)

// Btree metadata
type Btree struct {
	BtreeMetadata
	nodes       [][]byte
	dupnodelist map[int64]int
	opChan      chan *treeOperation
	exitChan    chan int
}

const (
	// TreeSize is  tree size
	TreeSize = 1 << 10
	// LeafSize is leaf size
	LeafSize = 1 << 5
	// NodeSize is node size
	NodeSize = 1 << 6
)

// isNode, isLeaf is treenode tag
const (
	isNode = iota
	isLeaf
)

// NewBtree create a btree
func NewBtree() *Btree {
	tree := &Btree{
		nodes:       make([][]byte, TreeSize),
		dupnodelist: make(map[int64]int),
		opChan:      make(chan *treeOperation),
		BtreeMetadata: BtreeMetadata{
			Root:        proto.Int64(0),
			Size:        proto.Int64(TreeSize),
			LeafMax:     proto.Int64(LeafSize),
			NodeMax:     proto.Int64(NodeSize),
			IndexCursor: proto.Int64(0),
		},
	}
	go tree.run()
	return tree
}

// NewBtreeSize create new btree with custom leafsize/nodesize
func NewBtreeSize(leafsize int64, nodesize int64) *Btree {
	tree := &Btree{
		nodes:       make([][]byte, TreeSize),
		dupnodelist: make(map[int64]int),
		opChan:      make(chan *treeOperation),
		BtreeMetadata: BtreeMetadata{
			Root:        proto.Int64(0),
			Size:        proto.Int64(TreeSize),
			LeafMax:     proto.Int64(leafsize),
			NodeMax:     proto.Int64(nodesize),
			IndexCursor: proto.Int64(0),
		},
	}
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
			switch op.GetAction() {
			case "insert":
				op.restChan <- t.insert(op.TreeLog)
			case "delete":
				op.restChan <- t.dodelete(op.Key)
			case "update":
				op.restChan <- t.update(op.TreeLog)
			case "search":
				rst, _ := t.search(op.Key)
				op.restChan <- rst
			}
		case <-tick:
			t.gc()
		}
	}
	//t.Marshal("treedump.tmp")
}

func (t *Btree) Sync(file string) {
	//t.Marshal(file)
}

// Insert can insert record into a btree
func (t *Btree) Insert(key, value []byte) bool {
	q := &treeOperation{
		restChan: make(chan interface{}),
	}
	q.Action = proto.String("insert")
	q.Key = key
	q.Value = value
	t.opChan <- q
	rst := <-q.restChan
	return rst.(bool)
}

// Delete can delete record
func (t *Btree) Delete(key []byte) error {
	q := &treeOperation{
		restChan: make(chan interface{}),
	}
	q.Action = proto.String("delete")
	q.Key = key
	t.opChan <- q
	rst := <-q.restChan
	return rst.(error)
}

// Search return value
func (t *Btree) Search(key []byte) []byte {
	q := &treeOperation{
		restChan: make(chan interface{}),
	}
	q.Action = proto.String("search")
	q.Key = key
	t.opChan <- q
	rst := <-q.restChan
	return rst.([]byte)
}

// Update is used to update key/value
func (t *Btree) Update(key, value []byte) error {
	q := &treeOperation{
		restChan: make(chan interface{}),
	}
	q.Action = proto.String("update")
	q.Key = key
	q.Value = value
	t.opChan <- q
	rst := <-q.restChan
	return rst.(error)
}
