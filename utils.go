package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
)

/*
 * alloc leaf/node
 */
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

func (this *Btree) newleaf() int32 {
	*this.LeafCount++
	leaf := &Leaf{
		IndexMetaData: IndexMetaData{Id: proto.Int32(this.genrateid()), Version: proto.Int32(this.GetVersion())},
	}
	this.nodes[leaf.GetId()] = leaf
	return leaf.GetId()
}

func (this *Btree) newnode() int32 {
	*this.NodeCount++
	node := &Node{
		IndexMetaData: IndexMetaData{Id: proto.Int32(this.genrateid()), Version: proto.Int32(this.GetVersion())},
	}
	this.nodes[node.GetId()] = node
	return node.GetId()
}

func remove(index int32, tree *Btree) {
	if node, ok := tree.nodes[index].(*Node); ok {
		tree.FreeList = append(tree.FreeList, node.GetId())
		*tree.NodeCount--
	}
	if leaf, ok := tree.nodes[index].(*Leaf); ok {
		tree.FreeList = append(tree.FreeList, leaf.GetId())
		*tree.LeafCount--
	}
	tree.nodes[index] = nil
}
func mark_dup(index int32, tree *Btree) {
	if tree.stat == 1 {
		if _, ok := tree.nodes[index].(*Node); ok {
			*tree.NodeCount--
		}
		if _, ok := tree.nodes[index].(*Leaf); ok {
			*tree.LeafCount--
		}
		tree.dupnodelist = append(tree.dupnodelist, index)
	} else {
		remove(index, tree)
	}
}
func get_node(id int32, tree *Btree) *Node {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node
	}
	return nil
}
func get_leaf(id int32, tree *Btree) *Leaf {
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf
	}
	return nil
}
func get_id(id int32, tree *Btree) *int32 {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node.Id
	}
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf.Id
	}
	return nil
}
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
func (this *Btree) clonenode(node *Node) *Node {
	newnode := get_node(this.newnode(), this)
	newnode.Keys = make([][]byte, len(node.Keys))
	copy(newnode.Keys, node.Keys)
	newnode.Childrens = make([]int32, len(node.Childrens))
	copy(newnode.Childrens, node.Childrens)
	return newnode
}
func (this *Btree) cloneleaf(leaf *Leaf) *Leaf {
	newleaf := get_leaf(this.newleaf(), this)
	newleaf.Keys = make([][]byte, len(leaf.Keys))
	copy(newleaf.Keys, leaf.Keys)
	newleaf.Values = make([][]byte, len(leaf.Values))
	copy(newleaf.Values, leaf.Values)
	return newleaf
}
func (this *Btree) gc() {
	for {
		if len(this.dupnodelist) > 0 && this.stat == 0 {
			id := this.dupnodelist[len(this.dupnodelist)-1]
			this.dupnodelist = this.dupnodelist[:len(this.dupnodelist)-1]
			remove(id, this)
		} else {
			break
		}
	}
}
