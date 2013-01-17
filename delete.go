package btree

import (
	"bytes"
)

// delete in cloned node/leaf
func (this *Node) delete_record(key []byte, tree *Btree) (bool, TreeNode, []byte) {
	index := this.locate(key)
	clone_node, _ := this.clone(tree).(*Node)
	if this.GetId() == tree.GetRoot() {
		tree.cloneroot = clone_node.GetId()
	}
	if rst, clone_treenode, new_key := tree.nodes[clone_node.Childrens[index]].delete_record(key, tree); rst {
		t_key := new_key
		if new_key != nil {
			if clone_node.replace(key, new_key) {
				new_key = nil
			}
		}
		id := *get_treenode_id(clone_treenode)
		clone_node.Childrens[index] = id
		tree.nodes[id] = clone_treenode
		if index == 0 {
			index = 1
		}
		if len(clone_node.Keys) > 0 {
			if get_leaf(clone_node.Childrens[index-1], tree) != nil {
				clone_node.merge_leaf(clone_node.Childrens[index-1], clone_node.Childrens[index], index-1, tree)
			} else {
				clone_node.merge_node(clone_node.Childrens[index-1], clone_node.Childrens[index], index-1, tree)
				if index == 1 && t_key == nil {
					node := get_node(clone_node.Childrens[0], tree)
					if node != nil {
						new_key = node.Keys[0]
					}
				}
			}
		}
		mark_dup(*this.Id, tree)
		return true, clone_node, new_key
	}
	return false, nil, nil
}

//delete record in a lead
//first return deleted or not
//second return clone_treenode
func (this *Leaf) delete_record(key []byte, tree *Btree) (bool, TreeNode, []byte) {
	deleted := false
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], key) == 0 {
			deleted = true
		}
	}
	if deleted {
		clone_leaf, _ := this.clone(tree).(*Leaf)
		clone_leaf.Keys = append(clone_leaf.Keys[:index], clone_leaf.Keys[index+1:]...)
		clone_leaf.Values = append(clone_leaf.Values[:index], clone_leaf.Values[index+1:]...)
		mark_dup(*this.Id, tree)
		if index == 0 && len(clone_leaf.Keys) > 0 {
			return true, clone_leaf, clone_leaf.Keys[0]
		} else {
			return true, clone_leaf, nil
		}
	}
	return false, nil, nil
}

// replace delete key
func (this *Node) replace(old_key, new_key []byte) bool {
	index := this.locate(old_key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], old_key) == 0 {
			this.Keys[index] = new_key
			return true
		}
	}
	return false
}
