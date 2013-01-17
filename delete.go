package btree

import (
	"bytes"
)

/*
 * Delete
 */
func delete(treenode TreeNode, key []byte, tree *Btree) (rst bool, refer int32) {
	var dup_id int32
	rst = false
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		if node.GetId() == tree.GetRoot() {
			tree.cloneroot = clonenode.GetId()
		}
		if rst = clonenode.delete(key, tree); rst {
			if node.GetId() == tree.GetRoot() {
				if len(clonenode.Keys) == 0 {
					tree.Root = get_id(clonenode.Childrens[0], tree)
					remove(tree.cloneroot, tree)
				} else {
					tree.Root = clonenode.Id
				}
			}
			dup_id = clonenode.GetId()
			mark_dup(node.GetId(), tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		if rst = cloneleaf.delete(key, tree); rst {
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

// delete in cloned node/leaf
func (this *Node) delete(key []byte, tree *Btree) bool {
	index := this.locate(key)
	rst, refer := delete(tree.nodes[this.Childrens[index]], key, tree)
	if rst {
		this.Childrens[index] = refer
		if index == 0 {
			index = 1
		}
		if len(this.Keys) > 0 {
			if get_node(this.Childrens[0], tree) != nil {
				this.mergenode(this.Childrens[index-1], this.Childrens[index], index-1, tree)
			} else {
				removed_key := this.Keys[0]
				this.mergeleaf(this.Childrens[index-1], this.Childrens[index], index-1, tree)
				if index == 1 {
					replace(key, removed_key, tree.cloneroot, tree)
				}
			}
		}
		return true
	}
	remove(this.GetId(), tree)
	return false
}
func (this *Leaf) delete(key []byte, tree *Btree) bool {
	var deleted bool
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], key) == 0 {
			deleted = true
		}
	}
	if deleted {
		this.Keys = append(this.Keys[:index], this.Keys[index+1:]...)
		this.Values = append(this.Values[:index], this.Values[index+1:]...)
		if index == 0 && len(this.Keys) > 0 {
			if tree.cloneroot != this.GetId() {
				replace(key, this.Keys[0], tree.cloneroot, tree)
			}
		}
		return true
	}
	remove(this.GetId(), tree)
	return false
}
