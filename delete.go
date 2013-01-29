package btree

import (
	"bytes"
)

// delete in cloned node/leaf
func (this *Node) delete_record(key []byte, tree *Btree) (bool, TreeNode, []byte) {
	index := this.locate(key)
	if rst, clone_treenode, new_key := tree.nodes[this.Childrens[index]].delete_record(key, tree); rst {
		clone_node, _ := this.clone(tree).(*Node)
		tree.nodes[clone_node.GetId()] = clone_node
		clone_node.Childrens[index] = *get_treenode_id(clone_treenode)
		if this.GetId() == tree.GetRoot() {
			tree.cloneroot = clone_node.GetId()
		}
		t_key := new_key
		if new_key != nil {
			if clone_node.replace(key, new_key) {
				new_key = nil
			}
		}
		if index == 0 {
			index = 1
		}
		if len(clone_node.Keys) > 0 {
			var left int32
			if get_leaf(clone_node.Childrens[index-1], tree) != nil {
				left = clone_node.merge_leaf(
					clone_node.Childrens[index-1],
					clone_node.Childrens[index],
					index-1,
					tree)
				if index == 1 && t_key == nil {
					leaf := get_leaf(
						clone_node.Childrens[0],
						tree)
					if leaf != nil && len(leaf.Keys) > 0 {
						new_key = leaf.Keys[0]
					}
				}
			} else {
				left = clone_node.merge_node(
					clone_node.Childrens[index-1],
					clone_node.Childrens[index],
					index-1,
					tree)
			}
			if left > 0 {
				clone_node.Childrens[index-1] = left
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
		clone_leaf.Keys = append(clone_leaf.Keys[:index],
			clone_leaf.Keys[index+1:]...)
		clone_leaf.Values = append(clone_leaf.Values[:index],
			clone_leaf.Values[index+1:]...)
		if this.GetId() == tree.GetRoot() {
			tree.cloneroot = clone_leaf.GetId()
		}
		tree.nodes[clone_leaf.GetId()] = clone_leaf
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
