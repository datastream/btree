package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"time"
)

// treenode interface
// node, leaf support this interface
type TreeNode interface {
	insertRecord(record *Record, tree *Btree) (bool, TreeNode)
	deleteRecord(key []byte, tree *Btree) (bool, TreeNode, []byte)
	updateRecord(recode *Record, tree *Btree) (bool, TreeNode)
	searchRecord(key []byte, tree *Btree) []byte
	clone(tree *Btree) TreeNode
	split(tree *Btree) (key []byte, left, right int32)
	locate(key []byte) int
}

// genrate node/leaf id
func (t *Btree) genrateID() int32 {
	var id int32
	if len(t.FreeList) > 0 {
		id = t.FreeList[len(t.FreeList)-1]
		t.FreeList = t.FreeList[:len(t.FreeList)-1]
	} else {
		if t.GetIndexCursor() >= t.GetSize() {
			t.nodes = append(t.nodes,
				make([]TreeNode, SIZE)...)
			*t.Size += int32(SIZE)
		}
		id = t.GetIndexCursor()
		*t.IndexCursor++
	}
	return id
}

//alloc new leaf
func (t *Btree) newLeaf() *Leaf {
	*t.LeafCount++
	leaf := &Leaf{
		IndexMetaData: IndexMetaData{Id: proto.Int32(t.genrateID()),
			Version: proto.Uint32(t.GetVersion())},
	}
	return leaf
}

//alloc new tree node
func (t *Btree) newNode() *Node {
	*t.NodeCount++
	node := &Node{
		IndexMetaData: IndexMetaData{Id: proto.Int32(t.genrateID()),
			Version: proto.Uint32(t.GetVersion())},
	}
	return node
}

//remove node/leaf
func remove(id int32, tree *Btree) {
	if node, ok := tree.nodes[id].(*Node); ok {
		tree.FreeList = append(tree.FreeList, node.GetId())
		*tree.NodeCount--
	}
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		tree.FreeList = append(tree.FreeList, leaf.GetId())
		*tree.LeafCount--
	}
	tree.nodes[id] = nil
}

//mark node/leaf duplicated
func markDup(id int32, tree *Btree) {
	if tree.isSyning {
		tree.gcLock.Lock()
		defer tree.gcLock.Unlock()
		tree.dupnodelist = append(tree.dupnodelist, id)

	}
}

//get node by id
func getNode(id int32, tree *Btree) *Node {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node
	}
	return nil
}

//get leaf by id
func getLeaf(id int32, tree *Btree) *Leaf {
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf
	}
	return nil
}

//get id by index
func getID(index int32, tree *Btree) *int32 {
	if node, ok := tree.nodes[index].(*Node); ok {
		return node.Id
	}
	if leaf, ok := tree.nodes[index].(*Leaf); ok {
		return leaf.Id
	}
	return nil
}

//get treenode's id
func getTreeNodeID(treenode TreeNode) *int32 {
	if node, ok := treenode.(*Node); ok {
		return node.Id
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.Id
	}
	return nil
}

//get key number
func getKeySize(treenode TreeNode) int {
	if node, ok := treenode.(*Node); ok {
		return len(node.Keys)
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return len(leaf.Keys)
	}
	return 0
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
		if len(t.dupnodelist) > 0 && !t.isSyning {
			t.gcLock.Lock()
			defer t.gcLock.Unlock()
			id := t.dupnodelist[len(t.dupnodelist)-1]
			t.dupnodelist = t.dupnodelist[:len(t.dupnodelist)-1]
			remove(id, t)
		} else {
			time.Sleep(time.Second)
		}
	}
}
