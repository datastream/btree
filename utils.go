package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
)

type TreeNode interface {
	insert_record(record *Record, tree *Btree) (bool, TreeNode)
	delete_record(key []byte, tree *Btree) (bool, TreeNode, []byte)
	update_record(recode *Record, tree *Btree) (bool, TreeNode)
	search_record(key []byte, tree *Btree) []byte
	clone(tree *Btree) TreeNode
	split(tree *Btree) (key []byte, left, right int32)
	locate(key []byte) int
}

// genrate node/leaf id
func (this *Btree) genrateid() int32 {
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

//alloc new leaf
func (this *Btree) newleaf() *Leaf {
	*this.LeafCount++
	leaf := &Leaf{
		IndexMetaData: IndexMetaData{Id: proto.Int32(this.genrateid()), Version: proto.Uint32(this.GetVersion())},
	}
	return leaf
}

//alloc new tree node
func (this *Btree) newnode() *Node {
	*this.NodeCount++
	node := &Node{
		IndexMetaData: IndexMetaData{Id: proto.Int32(this.genrateid()), Version: proto.Uint32(this.GetVersion())},
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
func mark_dup(id int32, tree *Btree) {
	if tree.is_syning {
		tree.dupnodelist = append(tree.dupnodelist, id)
	} else {
		remove(id, tree)
	}
}

//get node by id
func get_node(id int32, tree *Btree) *Node {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node
	}
	return nil
}

//get leaf by id
func get_leaf(id int32, tree *Btree) *Leaf {
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf
	}
	return nil
}

//get id by index
func get_id(index int32, tree *Btree) *int32 {
	if node, ok := tree.nodes[index].(*Node); ok {
		return node.Id
	}
	if leaf, ok := tree.nodes[index].(*Leaf); ok {
		return leaf.Id
	}
	return nil
}

//get treenode's id
func get_treenode_id(treenode TreeNode) *int32 {
	if node, ok := treenode.(*Node); ok {
		return node.Id
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.Id
	}
	return nil
}

//get key number
func get_key_size(treenode TreeNode) int {
	if node, ok := treenode.(*Node); ok {
		return len(node.Keys)
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return len(leaf.Keys)
	}
	return 0
}

//locate key's index in a node
func (this *Node) locate(key []byte) int {
	i := 0
	size := len(this.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(this.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}

//locate key's index in a leaf
func (this *Leaf) locate(key []byte) int {
	i := 0
	size := len(this.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(this.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}

//clone node
func (this *Node) clone(tree *Btree) TreeNode {
	newnode := tree.newnode()
	newnode.Keys = make([][]byte, len(this.Keys))
	copy(newnode.Keys, this.Keys)
	newnode.Childrens = make([]int32, len(this.Childrens))
	copy(newnode.Childrens, this.Childrens)
	return newnode
}

//clone leaf
func (this *Leaf) clone(tree *Btree) TreeNode {
	newleaf := tree.newleaf()
	newleaf.Keys = make([][]byte, len(this.Keys))
	copy(newleaf.Keys, this.Keys)
	newleaf.Values = make([][]byte, len(this.Values))
	copy(newleaf.Values, this.Values)
	return newleaf
}

//free dupnode
func (this *Btree) gc() {
	for {
		if len(this.dupnodelist) > 0 && !this.is_syning {
			id := this.dupnodelist[len(this.dupnodelist)-1]
			this.dupnodelist = this.dupnodelist[:len(this.dupnodelist)-1]
			remove(id, this)
		} else {
			break
		}
	}
}
