package btree

import (
	"bytes"
)

//insert
func (this *Node) insert_record(record *Record, tree *Btree) (bool, TreeNode) {
	index := this.locate(record.Key)
	if rst, clone_treenode := tree.nodes[this.Childrens[index]].insert_record(record, tree); rst {
		clone_node, _ := this.clone(tree).(*Node)
		clone_node.Childrens[index] = *get_treenode_id(clone_treenode)
		if get_key_size(clone_treenode) > int(tree.GetNodeMax()) {
			key, left, right := clone_treenode.split(tree)
			clone_node.insert_once(key, left, right, tree)
		}
		tree.nodes[*get_treenode_id(clone_treenode)] = clone_treenode
		mark_dup(*this.Id, tree)
		return true, clone_node
	}
	return false, nil
}
func (this *Leaf) insert_record(record *Record, tree *Btree) (bool, TreeNode) {
	index := this.locate(record.Key)
	if index > 0 {
		if bytes.Compare(this.Keys[index-1], record.Key) == 0 {
			return false, nil
		}
	}
	var clone_leaf *Leaf
	if tree.GetRoot() == *this.Id && len(this.Keys) == 0 {
		clone_leaf = this
	} else {
		clone_leaf, _ = this.clone(tree).(*Leaf)
	}
	clone_leaf.Keys = append(clone_leaf.Keys[:index], append([][]byte{record.Key}, clone_leaf.Keys[index:]...)...)
	clone_leaf.Values = append(clone_leaf.Values[:index], append([][]byte{record.Value}, clone_leaf.Values[index:]...)...)
	mark_dup(*this.Id, tree)
	return true, clone_leaf
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
