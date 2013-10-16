package btree

import (
	"bytes"
)

// insert node
func (n *Node) insertRecord(record *Record, tree *Btree) (bool, TreeNode) {
	index := n.locate(record.Key)
	if rst, clonedTreeNode := tree.nodes[n.Childrens[index]].insertRecord(record, tree); rst {
		clonedNode, _ := n.clone(tree).(*Node)
		clonedNode.Childrens[index] = clonedTreeNode.GetId()
		if len(clonedTreeNode.GetKeys()) > int(tree.GetNodeMax()) {
			key, left, right := clonedTreeNode.split(tree)
			clonedNode.insertOnce(key, left, right, tree)
		}
		tree.markDup(n.GetId())
		return true, clonedNode
	}
	return false, nil
}

// insert leaf
func (l *Leaf) insertRecord(record *Record, tree *Btree) (bool, TreeNode) {
	index := l.locate(record.Key)
	if index > 0 {
		if bytes.Compare(l.Keys[index-1], record.Key) == 0 {
			return false, nil
		}
	}
	var clonedLeaf *Leaf
	if len(l.Keys) == 0 {
		clonedLeaf = l
	} else {
		clonedLeaf, _ = l.clone(tree).(*Leaf)
		tree.markDup(l.GetId())
	}
	clonedLeaf.Keys = append(clonedLeaf.Keys[:index],
		append([][]byte{record.Key}, clonedLeaf.Keys[index:]...)...)
	clonedLeaf.Values = append(clonedLeaf.Values[:index],
		append([][]byte{record.Value}, clonedLeaf.Values[index:]...)...)
	return true, clonedLeaf
}

// Insert key into tree node
func (n *Node) insertOnce(key []byte, leftID int32, rightID int32, tree *Btree) {
	index := n.locate(key)
	if len(n.Keys) == 0 {
		n.Childrens = append([]int32{leftID}, rightID)
	} else {
		n.Childrens = append(n.Childrens[:index+1],
			append([]int32{rightID}, n.Childrens[index+1:]...)...)
	}
	n.Keys = append(n.Keys[:index], append([][]byte{key}, n.Keys[index:]...)...)
}
