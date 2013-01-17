package btree

import (
	"bytes"
)

/*
 * Update
 */
func update(treenode TreeNode, record *Record, tree *Btree) (rst bool, refer int32) {
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		rst = clonenode.update(record, tree)
		if rst {
			refer = clonenode.GetId()
			if tree.GetRoot() == node.GetId() {
				tree.Root = clonenode.Id
			}
			mark_dup(node.GetId(), tree)
		}
		return
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst = cloneleaf.update(record, tree)
		if rst {
			refer = cloneleaf.GetId()
			if tree.GetRoot() == leaf.GetId() {
				tree.Root = cloneleaf.Id
			}
			mark_dup(leaf.GetId(), tree)
		}
		return
	}
	return
}
func (this *Node) update(record *Record, tree *Btree) bool {
	index := this.locate(record.Key)
	stat, clone := update(tree.nodes[this.Childrens[index]], record, tree)
	if stat {
		this.Childrens[index] = clone
	} else {
		remove(this.GetId(), tree)
	}
	return stat
}

func (this *Leaf) update(record *Record, tree *Btree) bool {
	index := this.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], record.Key) == 0 {
			this.Values[index] = record.Value
			return true
		}
	}
	remove(this.GetId(), tree)
	return false
}

/*
 * Replace key in node
 */
func replace(oldkey []byte, newkey []byte, id int32, tree *Btree) {
	node := get_node(id, tree)
	if node != nil {
		index := node.locate(oldkey) - 1
		if index >= 0 {
			if bytes.Compare(node.Keys[index], oldkey) == 0 {
				node.Keys[index] = newkey
				return
			} else {
				replace(oldkey, newkey, node.Childrens[index+1], tree)
			}
		}
	}
}
