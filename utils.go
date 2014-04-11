package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"sync/atomic"
)

type treeOperation struct {
	TreeLog
	valueChan chan []byte
	errChan   chan error
}

// genrate node/leaf id
func (t *Btree) genrateID() int64 {
	var id int64
	id = -1
	for k, _ := range t.dupnodelist {
		id = k
		delete(t.dupnodelist, k)
		break
	}
	if id == -1 {
		if t.GetIndexCursor() >= t.GetSize() {
			t.Nodes = append(t.Nodes, make([][]byte, TreeSize)...)
			*t.Size += int64(TreeSize)
		}
		id = t.GetIndexCursor()
		*t.IndexCursor++
	}
	return id
}

//alloc new tree node
func (t *Btree) newTreeNode() *TreeNode {
	id := t.genrateID()
	node := &TreeNode{
		Id:     proto.Int64(id),
		IsDirt: proto.Int32(0),
	}
	return node
}

func (t *Btree) getTreeNode(id int64) (*TreeNode, error) {
	var tnode TreeNode
	var err error
	if len(t.Nodes[id]) > 0 {
		err = proto.Unmarshal(t.Nodes[id], &tnode)
	} else {
		err = fmt.Errorf("no data")
	}
	return &tnode, err
}

//locate key's index in a node
func (n *TreeNode) locate(key []byte) int {
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

//clone node
func (n *TreeNode) clone(tree *Btree) *TreeNode {
	nnode := tree.newTreeNode()
	nnode.Keys = n.GetKeys()
	nnode.Childrens = n.GetChildrens()
	nnode.Values = n.GetValues()
	nnode.NodeType = proto.Int32(n.GetNodeType())
	atomic.StoreInt32(n.IsDirt, 1)
	tree.Nodes[n.GetId()], _ = proto.Marshal(n)
	return nnode
}

//gc dupnodelist
func (t *Btree) gc() {
	for _, n := range t.Nodes {
		var v TreeNode
		err := proto.Unmarshal(n, &v)
		if err == nil && v.isReleaseAble() {
			t.dupnodelist[v.GetId()] = 1
		}
	}
}

func (n *TreeNode) isReleaseAble() bool {
	if atomic.LoadInt32(n.IsDirt) > 0 {
		return true
	}
	return false
}
