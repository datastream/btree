package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"sync"
)

// if btree's stat >0, btree is inserting/deleteing
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
	leaf := tree.newleaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.nodes[*tree.Root] = leaf
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
	leaf := tree.newleaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.nodes[*tree.Root] = leaf
	return tree
}

func (this *Btree) Insert(record *Record, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.Version++
		this.stat++
	}
	stat, clone_treenode := this.nodes[this.GetRoot()].insert_record(record, this)
	if stat {
		if get_key_size(clone_treenode) > int(this.GetNodeMax()) {
			new_node := this.newnode()
			key, left, right := clone_treenode.split(this)
			new_node.insert_once(key, left, right, this)
			this.Root = get_treenode_id(new_node)
			this.nodes[int(this.GetRoot())] = new_node
		} else {
			this.Root = get_treenode_id(clone_treenode)
			this.nodes[int(this.GetRoot())] = clone_treenode
		}
	}
	rst <- stat
}

func (this *Btree) Delete(key []byte, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.Version++
		this.stat++
	}
	stat, clone_treenode, _ := this.nodes[this.GetRoot()].delete_record(key, this)
	if stat {
		if get_key_size(clone_treenode) == 0 {
			if clone_node, ok := clone_treenode.(*Node); ok {
				this.Root = get_id(clone_node.Childrens[0], this)
				mark_dup(*clone_node.Id, this)
			}
		} else {
			this.Root = get_treenode_id(clone_treenode)
		}
	}
	rst <- stat
}

func (this *Btree) Search(key []byte, rst chan []byte) {
	rst <- this.nodes[this.GetRoot()].search_record(key, this)
}

func (this *Btree) Update(record *Record, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.Version++
		this.stat++
	}
	stat, clone_treenode := this.nodes[this.GetRoot()].update_record(record, this)
	if stat {
		mark_dup(this.GetRoot(), this)
		this.Root = get_treenode_id(clone_treenode)
		this.nodes[int(this.GetRoot())] = clone_treenode
	}
	rst <- stat
}
