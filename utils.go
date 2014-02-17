package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"sync/atomic"
)

type treeOperation struct {
	Action   string
	Payload  interface{}
	restChan chan interface{}
}

// TreeNode interface
// node, leaf support this interface
type TreeNode interface {
	insertRecord(record *Record, tree *Btree) (bool, TreeNode)
	deleteRecord(key []byte, tree *Btree) (bool, TreeNode, []byte)
	updateRecord(recode *Record, tree *Btree) (bool, TreeNode)
	searchRecord(key []byte, tree *Btree) []byte
	split(tree *Btree) (key []byte, left, right int64)
	locate(key []byte) int
	GetId() int64
	GetKeys() [][]byte
	isReleaseAble() bool
}

// genrate node/leaf id
func (t *Btree) genrateID() int64 {
	var id int64
	size := len(t.dupnodelist)
	if size > 0 {
		id = t.dupnodelist[size-1]
		t.dupnodelist = t.dupnodelist[:size-1]
	} else {
		if t.GetIndexCursor() >= t.GetSize() {
			t.nodes = append(t.nodes, make([]TreeNode, TreeSize)...)
			*t.Size += int64(TreeSize)
		}
		id = t.GetIndexCursor()
		*t.IndexCursor++
	}
	return id
}

//alloc new leaf
func (t *Btree) newLeaf() *Leaf {
	*t.LeafCount++
	id := t.genrateID()
	leaf := &Leaf{
		LeafRecordMetaData: LeafRecordMetaData{
			Id:     proto.Int64(id),
			IsDirt: proto.Int32(0),
		},
	}
	t.nodes[id] = leaf
	return leaf
}

//alloc new tree node
func (t *Btree) newNode() *Node {
	*t.NodeCount++
	id := t.genrateID()
	node := &Node{
		NodeRecordMetaData: NodeRecordMetaData{
			Id:     proto.Int64(id),
			IsDirt: proto.Int32(0),
		},
	}
	t.nodes[id] = node
	return node
}

//get node by id
func (t *Btree) getNode(id int64) *Node {
	if node, ok := t.nodes[id].(*Node); ok {
		return node
	}
	return nil
}

//get leaf by id
func (t *Btree) getLeaf(id int64) *Leaf {
	if leaf, ok := t.nodes[id].(*Leaf); ok {
		return leaf
	}
	return nil
}

//locate key's index in a node
func (n *Node) locate(key []byte) int {
	i := 0
	size := len(n.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(n.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}

//locate key's index in a leaf
func (l *Leaf) locate(key []byte) int {
	i := 0
	size := len(l.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(l.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}

//clone node
func (n *Node) clone(tree *Btree) TreeNode {
	nnode := tree.newNode()
	nnode.Keys = make([][]byte, len(n.Keys))
	copy(nnode.Keys, n.Keys)
	nnode.Childrens = make([]int64, len(n.Childrens))
	copy(nnode.Childrens, n.Childrens)
	atomic.StoreInt32(n.IsDirt, 1)
	return nnode
}

//clone leaf
func (l *Leaf) clone(tree *Btree) TreeNode {
	nleaf := tree.newLeaf()
	nleaf.Keys = make([][]byte, len(l.Keys))
	copy(nleaf.Keys, l.Keys)
	nleaf.Values = make([][]byte, len(l.Values))
	copy(nleaf.Values, l.Values)
	atomic.StoreInt32(l.IsDirt, 1)
	return nleaf
}

//gc dupnodelist
func (t *Btree) gc() {
	for _, v := range t.nodes {
		if v != nil && v.isReleaseAble() {
			t.dupnodelist = append(t.dupnodelist, v.GetId())
		}
	}
}

func (n *Node) isReleaseAble() bool {
	if atomic.CompareAndSwapInt32(n.IsDirt, 1, 0) {
		return true
	}
	return false
}

func (l *Leaf) isReleaseAble() bool {
	if atomic.CompareAndSwapInt32(l.IsDirt, 1, 0){
		return true
	}
	return false
}
