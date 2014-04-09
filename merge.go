package btree

import (
	"code.google.com/p/goprotobuf/proto"
	"sync/atomic"
)

func (n *TreeNode) merge(tree *Btree, index int) int64 {
	left, err := tree.getTreeNode(n.Childrens[index])
	if err != nil {
		return -1
	}
	right, err := tree.getTreeNode(n.Childrens[index+1])
	if err != nil {
		return -1
	}
	if len(left.Keys)+len(right.Keys) > int(tree.GetNodeMax()) {
		return -1
	}
	if (len(left.Values) + len(right.Values)) > int(tree.GetLeafMax()) {
		return -1
	}
	leftClone := left.clone(tree)
	n.Childrens[index] = leftClone.GetId()
	if leftClone.GetNodeType() == isLeaf {
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
	} else {
		leftClone.Keys = append(leftClone.Keys, append([][]byte{n.Keys[index]}, right.Keys...)...)
		// merge childrens
		leftClone.Childrens = append(leftClone.Childrens, right.Childrens...)
		// remove old key
		n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
		// remove old right node
		n.Childrens = append(n.Childrens[:index+1], n.Childrens[index+2:]...)
		// check size, spilt if over size
		if len(leftClone.Keys) > int(tree.GetNodeMax()) {
			key, left, right := leftClone.split(tree)
			n.insertOnce(key, left, right, tree)
		}
	}
	atomic.StoreInt32(right.IsDirt, 1)
	atomic.StoreInt32(left.IsDirt, 1)
	tree.nodes[right.GetId()], err = proto.Marshal(right)
	tree.nodes[left.GetId()], err = proto.Marshal(left)
	tree.nodes[leftClone.GetId()], err = proto.Marshal(leftClone)
	return leftClone.GetId()
}
