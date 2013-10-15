package btree

import (
	"bytes"
)

// update node
func (n *Node) updateRecord(record *Record, tree *Btree) (bool, TreeNode) {
	index := n.locate(record.Key)
	if stat, clonedTreeNode := tree.nodes[n.Childrens[index]].updateRecord(record, tree); stat {
		clonedNode, _ := n.clone(tree).(*Node)
		id := *getTreeNodeID(clonedTreeNode)
		clonedNode.Childrens[index] = id
		tree.nodes[id] = clonedTreeNode
		markDup(*n.Id, tree)
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
			markDup(*l.Id, tree)
			return true, clonedLeaf
		}
	}
	return false, nil
}
