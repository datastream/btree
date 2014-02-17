package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
)

// Update is used to update key/value
func (t *Btree) update(record *Record) bool {
	rst, clonedNode := t.nodes[t.GetRoot()].updateRecord(record, t)
	if rst {
		t.Root = proto.Int64(clonedNode.GetId())
	}
	return rst
}

// update node
func (n *Node) updateRecord(record *Record, tree *Btree) (bool, TreeNode) {
	index := n.locate(record.Key)
	if stat, clonedTreeNode := tree.nodes[n.Childrens[index]].updateRecord(record, tree); stat {
		clonedNode, _ := n.clone(tree).(*Node)
		clonedNode.Childrens[index] = clonedTreeNode.GetId()
		return true, clonedNode
	}
	return false, nil
}

// update leaf
func (l *Leaf) updateRecord(record *Record, tree *Btree) (bool, TreeNode) {
	index := l.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(l.Keys[index], record.Key) == 0 {
			clonedLeaf, _ := l.clone(tree).(*Leaf)
			clonedLeaf.Values[index] = record.Value
			return true, clonedLeaf
		}
	}
	return false, nil
}
