package btree

import (
	"bytes"
)

/*
 * Insert
 */
func insert(treenode TreeNode, record *Record, tree *Btree) (rst, split bool, key []byte, left, right, refer int32) {
	var dup_id int32
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		rst = clonenode.insert(record, tree)
		if len(clonenode.Keys) > int(tree.GetNodeMax()) {
			key, left, right = clonenode.split(tree)
			if node.GetId() == tree.GetRoot() {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, left, right, tree)
				tree.Root = tnode.Id
			} else {
				split = true
			}
		}
		if rst {
			if node.GetId() == tree.GetRoot() {
				tree.Root = clonenode.Id
			}
			dup_id = clonenode.GetId()
			mark_dup(node.GetId(), tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst = cloneleaf.insert(record, tree)
		if len(cloneleaf.Values) > int(tree.GetLeafMax()) {
			key, left, right = cloneleaf.split(tree)
			if leaf.GetId() == tree.GetRoot() {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, left, right, tree)
				tree.Root = tnode.Id
			} else {
				split = true
			}
		}
		if rst {
			if leaf.GetId() == tree.GetRoot() {
				tree.Root = cloneleaf.Id
			}
			dup_id = cloneleaf.GetId()
			mark_dup(leaf.GetId(), tree)
		}
	}
	refer = dup_id
	return
}
func (this *Node) insert(record *Record, tree *Btree) bool {
	index := this.locate(record.Key)
	rst, split, key, left, right, refer := insert(tree.nodes[this.Childrens[index]], record, tree)
	if rst {
		this.Childrens[index] = refer
		if split {
			this.insert_once(key, left, right, tree)
		}
	} else {
		remove(this.GetId(), tree)
	}
	return rst
}
func (this *Leaf) insert(record *Record, tree *Btree) bool {
	index := this.locate(record.Key)
	if index > 0 {
		if bytes.Compare(this.Keys[index-1], record.Key) == 0 {
			remove(this.GetId(), tree)
			return false
		}
	}
	this.Keys = append(this.Keys[:index], append([][]byte{record.Key}, this.Keys[index:]...)...)
	this.Values = append(this.Values[:index], append([][]byte{record.Value}, this.Values[index:]...)...)
	return true
}

/*
 * Insert key into tree node
 */
func (this *Node) insert_once(key []byte, left_id int32, right_id int32, tree *Btree) {
	index := this.locate(key)
	if len(this.Keys) == 0 {
		this.Childrens = append([]int32{left_id}, right_id)
	} else {
		this.Childrens = append(this.Childrens[:index+1], append([]int32{right_id}, this.Childrens[index+1:]...)...)
	}
	this.Keys = append(this.Keys[:index], append([][]byte{key}, this.Keys[index:]...)...)
}
