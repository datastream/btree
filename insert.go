package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
)

// Insert can insert record into a btree
func (t *Btree) insert(record *Record) bool {
	rst, clonedTreeNode := t.nodes[t.GetRoot()].insertRecord(record, t)
	if rst {
		var newroot TreeNode
		if len(clonedTreeNode.GetKeys()) > int(t.GetNodeMax()) {
			nnode := t.newNode()
			key, left, right := clonedTreeNode.split(t)
			nnode.insertOnce(key, left, right, t)
			newroot = nnode
		} else {
			newroot = clonedTreeNode
		}
		t.Root = proto.Int64(newroot.GetId())
	}
	return rst
}

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
	}
	clonedLeaf.Keys = append(clonedLeaf.Keys[:index],
		append([][]byte{record.Key}, clonedLeaf.Keys[index:]...)...)
	clonedLeaf.Values = append(clonedLeaf.Values[:index],
		append([][]byte{record.Value}, clonedLeaf.Values[index:]...)...)
	return true, clonedLeaf
}

// Insert key into tree node
func (n *Node) insertOnce(key []byte, leftID int64, rightID int64, tree *Btree) {
	index := n.locate(key)
	if len(n.Keys) == 0 {
		n.Childrens = append([]int64{leftID}, rightID)
	} else {
		n.Childrens = append(n.Childrens[:index+1],
			append([]int64{rightID}, n.Childrens[index+1:]...)...)
	}
	n.Keys = append(n.Keys[:index], append([][]byte{key}, n.Keys[index:]...)...)
}
