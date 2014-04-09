package btree

import (
	"code.google.com/p/goprotobuf/proto"
)

func (n *TreeNode) split(tree *Btree) (key []byte, left, right int64) {
	nnode := tree.newTreeNode()
	nnode.NodeType = proto.Int32(n.GetNodeType())
	if n.GetNodeType() == isLeaf {
		mid := tree.GetLeafMax() / 2
		nnode.Values = n.GetValues()[mid:]
		nnode.Keys = n.GetKeys()[mid:]
		key = nnode.Keys[0]
		n.Keys = n.Keys[:mid]
		n.Values = n.Values[:mid]
	} else {
		mid := tree.GetNodeMax() / 2
		key = n.Keys[mid]
		nnode.Keys = n.GetKeys()[mid+1:]
		nnode.Childrens = n.GetChildrens()[mid+1:]
		n.Keys = n.Keys[:mid]
		n.Childrens = n.Childrens[:mid+1]
	}
	left = n.GetId()
	right = nnode.GetId()
	tree.nodes[nnode.GetId()], _ = proto.Marshal(nnode)
	tree.nodes[n.GetId()], _ = proto.Marshal(n)
	return
}
