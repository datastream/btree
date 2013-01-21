package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"sync"
)

// if btree's stat >0, btree is inserting/deleteing
type Btree struct {
	BtreeMetaData
	sync.Mutex
	gc_lock     sync.RWMutex
	nodes       []TreeNode
	is_syning   bool
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

//create new record
func NewRecord(key, value []byte) *Record {
	return &Record{key, value}
}

//create new btree
func NewBtree() *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.is_syning = false
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(SIZE),
		LeafMax:     proto.Int32(LEAFSIZE),
		NodeMax:     proto.Int32(NODESIZE),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Uint32(0)
	leaf := tree.newleaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.nodes[*tree.Root] = leaf
	go tree.gc()
	return tree
}

//create new btree with custom leafsize/nodesize
func NewBtreeSize(leafsize int32, nodesize int32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.is_syning = false
	tree.BtreeMetaData = BtreeMetaData{
		Size:        proto.Int32(SIZE),
		LeafMax:     proto.Int32(leafsize),
		NodeMax:     proto.Int32(nodesize),
		LeafCount:   proto.Int32(0),
		NodeCount:   proto.Int32(0),
		IndexCursor: proto.Int32(0),
	}
	tree.Version = proto.Uint32(0)
	leaf := tree.newleaf()
	tree.Root = proto.Int32(leaf.GetId())
	tree.nodes[*tree.Root] = leaf
	go tree.gc()
	return tree
}

//insert
func (this *Btree) Insert(record *Record) bool {
	this.Lock()
	defer this.Unlock()
	*this.Version++
	stat, clone_treenode := this.nodes[this.GetRoot()].insert_record(record, this)
	if stat {
		this.nodes[*get_treenode_id(clone_treenode)] = clone_treenode
		if get_key_size(clone_treenode) > int(this.GetNodeMax()) {
			new_node := this.newnode()
			key, left, right := clone_treenode.split(this)
			new_node.insert_once(key, left, right, this)
			this.Root = get_treenode_id(new_node)
			this.nodes[int(this.GetRoot())] = new_node
		} else {
			this.Root = get_treenode_id(clone_treenode)
		}
	} else {
		*this.Version--
	}
	return stat
}

//delete
func (this *Btree) Delete(key []byte) bool {
	this.Lock()
	defer this.Unlock()
	*this.Version++
	stat, clone_treenode, _ := this.nodes[this.GetRoot()].delete_record(key, this)
	if stat {
		this.nodes[*get_treenode_id(clone_treenode)] = clone_treenode
		if get_key_size(clone_treenode) == 0 {
			if clone_node, ok := clone_treenode.(*Node); ok {
				this.Root = get_id(clone_node.Childrens[0], this)
				mark_dup(*clone_node.Id, this)
			} else {
				this.Root = get_treenode_id(clone_treenode)
			}
		} else {
			this.Root = get_treenode_id(clone_treenode)
		}
	} else {
		*this.Version--
	}
	return stat
}

//search
func (this *Btree) Search(key []byte) []byte {
	this.gc_lock.RLock()
	defer this.gc_lock.RUnlock()
	return this.nodes[this.GetRoot()].search_record(key, this)
}

//update
func (this *Btree) Update(record *Record) bool {
	this.Lock()
	defer this.Unlock()
	*this.Version++
	stat, clone_treenode := this.nodes[this.GetRoot()].update_record(record, this)
	if stat {
		this.nodes[*get_treenode_id(clone_treenode)] = clone_treenode
		mark_dup(this.GetRoot(), this)
		this.Root = get_treenode_id(clone_treenode)
	} else {
		*this.Version--
	}
	return stat
}
