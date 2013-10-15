package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"sync"
)

// if btree's stat >0, btree is inserting/deleteing
type Btree struct {
	BtreeMetaData
	sync.Mutex
	gcLock      sync.RWMutex
	nodes       []TreeNode
	isSyning    bool
	cloneroot   int32
	dupnodelist []int32
}

// tree node
type Node struct {
	IndexMetaData
	NodeRecordMetaData
}

// tree leaf
type Leaf struct {
	IndexMetaData
	LeafRecordMetaData
}

// data struct in leaf
type Record struct {
	Key   []byte
	Value []byte
}

// tree size
const SIZE = 1 << 10

// leaf size
const LEAFSIZE = 1 << 5

// node size
const NODESIZE = 1 << 6

// node, leaf setting
const (
	NODE = 1
	LEAF = 2
)

//create new record
func NewRecord(key, value []byte) *Record {
	return &Record{key, value}
}

//create new btree
func NewBtree() *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.isSyning = false
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(SIZE),
		LeafMax:     proto.Int32(LEAFSIZE),
		NodeMax:     proto.Int32(NODESIZE),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Uint32(0)
	leaf := tree.newLeaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.nodes[*tree.Root] = leaf
	go tree.gc()
	return tree
}

//create new btree with custom leafsize/nodesize
func NewBtreeSize(leafsize int32, nodesize int32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.isSyning = false
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(SIZE),
		LeafMax:     proto.Int32(leafsize),
		NodeMax:     proto.Int32(nodesize),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Uint32(0)
	leaf := tree.newLeaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.nodes[*tree.Root] = leaf
	go tree.gc()
	return tree
}

//insert
func (t *Btree) Insert(record *Record) bool {
	t.Lock()
	defer t.Unlock()
	*t.Version++
	stat, clonedTreeNode := t.nodes[t.GetRoot()].insertRecord(record, t)
	if stat {
		t.nodes[*getTreeNodeID(clonedTreeNode)] = clonedTreeNode
		if getKeySize(clonedTreeNode) > int(t.GetNodeMax()) {
			nnode := t.newNode()
			key, left, right := clonedTreeNode.split(t)
			nnode.insertOnce(key, left, right, t)
			t.Root = getTreeNodeID(nnode)
			t.nodes[int(t.GetRoot())] = nnode
		} else {
			t.Root = getTreeNodeID(clonedTreeNode)
		}
	} else {
		*t.Version--
	}
	return stat
}

//delete
func (t *Btree) Delete(key []byte) bool {
	t.Lock()
	defer t.Unlock()
	*t.Version++
	stat, clonedTreeNode, _ := t.nodes[t.GetRoot()].deleteRecord(key, t)
	if stat {
		t.nodes[*getTreeNodeID(clonedTreeNode)] = clonedTreeNode
		if getKeySize(clonedTreeNode) == 0 {
			if clonedNode, ok := clonedTreeNode.(*Node); ok {
				t.Root = getID(clonedNode.Childrens[0], t)
				markDup(*clonedNode.Id, t)
			} else {
				t.Root = getTreeNodeID(clonedTreeNode)
			}
		} else {
			t.Root = getTreeNodeID(clonedTreeNode)
		}
	} else {
		*t.Version--
	}
	return stat
}

//search
func (t *Btree) Search(key []byte) []byte {
	t.gcLock.RLock()
	defer t.gcLock.RUnlock()
	return t.nodes[t.GetRoot()].searchRecord(key, t)
}

//update
func (t *Btree) Update(record *Record) bool {
	t.Lock()
	defer t.Unlock()
	*t.Version++
	stat, clonedTreeNode := t.nodes[t.GetRoot()].updateRecord(record, t)
	if stat {
		t.nodes[*getTreeNodeID(clonedTreeNode)] = clonedTreeNode
		markDup(t.GetRoot(), t)
		t.Root = getTreeNodeID(clonedTreeNode)
	} else {
		*t.Version--
	}
	return stat
}
