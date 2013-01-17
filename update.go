package btree

import (
	"bytes"
)

func (this *Node) update_record(record *Record, tree *Btree) (bool, TreeNode) {
	index := this.locate(record.Key)
	if stat, clone_treenode := tree.nodes[this.Childrens[index]].update_record(record, tree); stat {
		clone_node, _ := this.clone(tree).(*Node)
		id := *get_treenode_id(clone_treenode)
		clone_node.Childrens[index] = id
		tree.nodes[id] = clone_treenode
		mark_dup(*this.Id, tree)
		return true, clone_node
	}
	return false, nil
}

func (this *Leaf) update_record(record *Record, tree *Btree) (bool, TreeNode) {
	index := this.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], record.Key) == 0 {
			clone_leaf, _ := this.clone(tree).(*Leaf)
			clone_leaf.Values[index] = record.Value
			mark_dup(*this.Id, tree)
			return true, clone_leaf
		}
	}
	return false, nil
}
