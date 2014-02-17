package btree

import (
	"sync/atomic"
)

// merge leaf
func (n *Node) mergeLeaf(leftID int64, rightID int64, index int, tree *Btree) int64 {
	left := tree.getLeaf(leftID)
	right := tree.getLeaf(rightID)
	if (len(left.Values) + len(right.Values)) > int(tree.GetLeafMax()) {
		return -1
	}
	// clone left child
	leftClone := left.clone(tree).(*Leaf)
	id := leftClone.GetId()
	tree.nodes[id] = leftClone
	n.Childrens[index] = id
	// remove rightID
	if index == len(n.Keys) {
		n.Childrens = n.Childrens[:index]
		n.Keys = n.Keys[:index-1]
	} else {
		n.Childrens = append(n.Childrens[:index+1], n.Childrens[index+2:]...)
		n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
	}
	// add right to left
	leftClone.Values = append(leftClone.Values, right.Values...)
	leftClone.Keys = append(leftClone.Keys, right.Keys...)
	// cleanup old data
	atomic.StoreInt32(tree.getLeaf(rightID).IsDirt, 1)
	return leftClone.GetId()
}

// merge node
func (n *Node) mergeNode(leftID int64, rightID int64, index int, tree *Btree) int64 {
	left := tree.getNode(leftID)
	right := tree.getNode(rightID)
	if len(left.Keys)+len(right.Keys) > int(tree.GetNodeMax()) {
		return -1
	}
	// clone left node
	leftClone := left.clone(tree).(*Node)
	id := leftClone.GetId()
	tree.nodes[id] = leftClone
	n.Childrens[index] = id
	// merge key
	leftClone.Keys = append(leftClone.Keys, append([][]byte{n.Keys[index]}, right.Keys...)...)
	// merge childrens
	leftClone.Childrens = append(leftClone.Childrens, right.Childrens...)
	// remove old key
	n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
	// remove old right node
	n.Childrens = append(n.Childrens[:index+1],
		n.Childrens[index+2:]...)
	// check size, spilt if over size
	if len(leftClone.Keys) > int(tree.GetNodeMax()) {
		key, left, right := leftClone.split(tree)
		n.insertOnce(key, left, right, tree)
	}
	// cleanup old
	atomic.StoreInt32(tree.getNode(rightID).IsDirt, 1)
	return leftClone.GetId()
}
