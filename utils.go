package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"time"
	"sync/atomic"
)

// TreeNode interface
// node, leaf support this interface
type TreeNode interface {
	insertRecord(record *Record, tree *Btree) (bool, TreeNode)
	deleteRecord(key []byte, tree *Btree) (bool, TreeNode, []byte)
	updateRecord(recode *Record, tree *Btree) (bool, TreeNode)
	searchRecord(key []byte, tree *Btree) []byte
	split(tree *Btree) (key []byte, left, right int32)
	locate(key []byte) int
	GetId() int32
	GetKeys() [][]byte
}

// genrate node/leaf id
func (t *Btree) genrateID() int32 {
	var id int32
	if len(t.FreeList) > 0 {
		id = t.FreeList[len(t.FreeList)-1]
		t.FreeList = t.FreeList[:len(t.FreeList)-1]
	} else {
		if t.GetIndexCursor() >= t.GetSize() {
			t.nodes = append(t.nodes, make([]TreeNode, TreeSize)...)
			*t.Size += int32(TreeSize)
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
		IndexMetaData: IndexMetaData{Id: proto.Int32(id),
			Version: proto.Uint32(t.GetVersion())},
	}
	t.nodes[id] = leaf
	return leaf
}

//alloc new tree node
func (t *Btree) newNode() *Node {
	*t.NodeCount++
	id := t.genrateID()
	node := &Node{
		IndexMetaData: IndexMetaData{Id: proto.Int32(id),
			Version: proto.Uint32(t.GetVersion())},
	}
	t.nodes[id] = node
	return node
}

//mark node/leaf duplicated
func (t *Btree)markDup(id int32) {
	t.dupnodelist = append(t.dupnodelist, id)
}

//get node by id
func (t *Btree)getNode(id int32) *Node {
	if node, ok := t.nodes[id].(*Node); ok {
		return node
	}
	return nil
}

//get leaf by id
func (t *Btree)getLeaf(id int32) *Leaf {
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
	nnode.Childrens = make([]int32, len(n.Childrens))
	copy(nnode.Childrens, n.Childrens)
	return nnode
}

//clone leaf
func (l *Leaf) clone(tree *Btree) TreeNode {
	nleaf := tree.newLeaf()
	nleaf.Keys = make([][]byte, len(l.Keys))
	copy(nleaf.Keys, l.Keys)
	nleaf.Values = make([][]byte, len(l.Values))
	copy(nleaf.Values, l.Values)
	return nleaf
}

//gc dupnodelist
func (t *Btree) gc() {
	for {
		t.Lock()
		if atomic.CompareAndSwapInt32(&t.state, StateNormal, StateGc) {
			if len(t.dupnodelist) > 0 {
				id := t.dupnodelist[len(t.dupnodelist)-1]
				switch t.nodes[id].(type) {
				case *Node:
					*t.NodeCount--
				case *Leaf:
					*t.LeafCount--
				default:
					atomic.CompareAndSwapInt32(&t.state, StateGc, StateNormal)
					continue
				}
				t.FreeList = append(t.FreeList, id)
				t.dupnodelist = t.dupnodelist[:len(t.dupnodelist)-1]
				atomic.CompareAndSwapInt32(&t.state, StateGc, StateNormal)
			}
		} else {
			time.Sleep(time.Second)
		}
		t.Unlock()
	}
}
