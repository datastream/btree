package btree

import (
	"bytes"
	"fmt"
)

// Search return value
func (t *Btree) search(key []byte) ([]byte, error) {
	var value []byte
	tnode, err := t.getTreeNode(t.GetRoot())
	if err != nil {
		return value, err
	}
	return tnode.searchRecord(key, t)
}

// node search record
func (n *TreeNode) searchRecord(key []byte, tree *Btree) ([]byte, error) {
	var value []byte
	index := n.locate(key)
	tnode, err := tree.getTreeNode(n.Childrens[index])
	if err != nil {
		return value, err
	}
	if tnode.GetNodeType() == isNode {
		return tnode.searchRecord(key, tree)
	} else {
		index--
		if index >= 0 {
			if bytes.Compare(n.Keys[index], key) == 0 {
				return n.Values[index], nil
			}
		}
	}
	return value, fmt.Errorf("%s not find", string(key))
}
