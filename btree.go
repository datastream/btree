package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"sync"
)

// Btree metadata
type Btree struct {
	BtreeMetaData
	sync.Mutex
	nodes       []TreeNode
	state       int32
	cloneroot   int32
	dupnodelist []int32
}

// Node is btree node
type Node struct {
	IndexMetaData
	NodeRecordMetaData
}

// Leaf is btree leaf
type Leaf struct {
	IndexMetaData
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

// StateInt, StateDump, StateSync is btree stat
const (
	StateNormal = iota
	StateDump
	StateGc
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
		Size:        proto.Int32(TreeSize),
		LeafMax:     proto.Int32(LeafSize),
		NodeMax:     proto.Int32(NodeSize),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Uint32(0)
	leaf := tree.newLeaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.state = StateNormal
	return tree
}

// NewBtreeSize create new btree with custom leafsize/nodesize
func NewBtreeSize(leafsize int32, nodesize int32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, TreeSize)
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(TreeSize),
		LeafMax:     proto.Int32(leafsize),
		NodeMax:     proto.Int32(nodesize),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Uint32(0)
	leaf := tree.newLeaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.state = StateNormal
	return tree
}

// Insert can insert record into a btree
func (t *Btree) Insert(record *Record) bool {
	t.Lock()
	defer t.Unlock()
	*t.Version++
	stat, clonedTreeNode := t.nodes[t.GetRoot()].insertRecord(record, t)
	if stat {
		if len(clonedTreeNode.GetKeys()) > int(t.GetNodeMax()) {
			nnode := t.newNode()
			key, left, right := clonedTreeNode.split(t)
			nnode.insertOnce(key, left, right, t)
			t.Root = proto.Int32(nnode.GetId())
			t.nodes[int(t.GetRoot())] = nnode
		} else {
			t.Root = proto.Int32(clonedTreeNode.GetId())
		}
	} else {
		*t.Version--
	}
	return stat
}

// Delete can delete record
func (t *Btree) Delete(key []byte) bool {
	t.Lock()
	defer t.Unlock()
	*t.Version++
	stat, clonedTreeNode, _ := t.nodes[t.GetRoot()].deleteRecord(key, t)
	if stat {
		if len(clonedTreeNode.GetKeys()) == 0 {
			if clonedNode, ok := clonedTreeNode.(*Node); ok {
				t.Root = proto.Int32(t.nodes[clonedNode.Childrens[0]].GetId())
				t.markDup(clonedNode.GetId())
			} else {
				t.Root = proto.Int32(clonedTreeNode.GetId())
			}
		} else {
			t.Root = proto.Int32(clonedTreeNode.GetId())
		}
	} else {
		*t.Version--
	}
	return stat
}

// Search return value
func (t *Btree) Search(key []byte) []byte {
	t.Lock()
	defer t.Unlock()
	return t.nodes[t.GetRoot()].searchRecord(key, t)
}

// Update is used to update key/value
func (t *Btree) Update(record *Record) bool {
	t.Lock()
	defer t.Unlock()
	*t.Version++
	stat, clonedNode := t.nodes[t.GetRoot()].updateRecord(record, t)
	if stat {
		t.markDup(t.GetRoot())
		t.Root = proto.Int32(clonedNode.GetId())
	} else {
		*t.Version--
	}
	return stat
}
