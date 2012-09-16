package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"sync"
)

type Btree struct {
	BtreeMetaData
	sync.Mutex
	nodes       []TreeNode
	stat        int
	cloneroot   int32
	dupnodelist []int32
}

type Node struct {
	IndexMetaData
	NodeRecordMetaData
}

type Leaf struct {
	IndexMetaData
	LeafRecordMetaData
}

type Record struct {
	Key   []byte
	Value []byte
}

const SIZE = 1 << 10
const LEAFSIZE = 1 << 5
const NODESIZE = 1 << 6

const (
	NODE = 1
	LEAF = 2
)

type TreeNode interface {
	insert(record *Record, tree *Btree) bool
	delete(key []byte, tree *Btree) bool
	update(recode *Record, tree *Btree) bool
	search(key []byte, tree *Btree) []byte
}

func NewBtree() *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.stat = 0
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(SIZE),
		LeafMax:     proto.Int32(LEAFSIZE),
		NodeMax:     proto.Int32(NODESIZE),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Int32(0)
	tree.Root = proto.Int32(tree.newleaf())
	return tree
}

func NewBtreeSize(leafsize uint32, nodesize uint32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.stat = 0
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(SIZE),
		LeafMax:     proto.Int32(LEAFSIZE),
		NodeMax:     proto.Int32(NODESIZE),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Int32(0)
	tree.Root = proto.Int32(tree.newleaf())
	return tree
}

/*
 * alloc leaf/node
 */
func (this *Btree) GenrateId() int32 {
	var id int32
	if len(this.FreeList) > 0 {
		id = this.FreeList[len(this.FreeList)-1]
		this.FreeList = this.FreeList[:len(this.FreeList)-1]
	} else {
		if this.GetIndexCursor() >= this.GetSize() {
			this.nodes = append(this.nodes, make([]TreeNode, SIZE)...)
			*this.Size += int32(SIZE)
		}
		id = this.GetIndexCursor()
		*this.IndexCursor++
	}
	return id
}

func (this *Btree) newleaf() int32 {
	*this.LeafCount++
	leaf := &Leaf{
		IndexMetaData: IndexMetaData{Id: proto.Int32(this.GenrateId()), Version: proto.Int32(this.GetVersion())},
	}
	this.nodes[leaf.GetId()] = leaf
	return leaf.GetId()
}

func (this *Btree) newnode() int32 {
	*this.NodeCount++
	node := &Node{
		IndexMetaData: IndexMetaData{Id: proto.Int32(this.GenrateId()), Version: proto.Int32(this.GetVersion())},
	}
	this.nodes[node.GetId()] = node
	return node.GetId()
}

func (this *Btree) Insert(record *Record, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.Version++
		this.stat++
	}
	stat, _, _, _, _, _ := insert(this.nodes[this.GetRoot()], record, this)
	rst <- stat
}

func (this *Btree) Delete(key []byte, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.Version++
		this.stat++
	}
	stat, _ := delete(this.nodes[this.GetRoot()], key, this)
	rst <- stat
}

func (this *Btree) Search(key []byte, rst chan []byte) {
	rst <- search(this.nodes[this.GetRoot()], key, this)
}

func (this *Btree) Update(record *Record, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.Version++
		this.stat++
	}
	stat, _ := update(this.nodes[this.GetRoot()], record, this)
	rst <- stat
}
