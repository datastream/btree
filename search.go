package btree

import (
	"bytes"
)

/*
 * Search
 */
func search(treenode TreeNode, key []byte, tree *Btree) []byte {
	if node, ok := treenode.(*Node); ok {
		return node.search(key, tree)
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.search(key, tree)
	}
	return nil

}
func (this *Node) search(key []byte, tree *Btree) []byte {
	index := this.locate(key)
	return search(tree.nodes[this.Childrens[index]], key, tree)
}

func (this *Leaf) search(key []byte, tree *Btree) []byte {
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], key) == 0 {
			return this.Values[index]
		}
	}
	return nil
}
