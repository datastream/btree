package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
)

// Update is used to update key/value
func (t *Btree) update(record TreeLog) error {
	tnode, err := t.getTreeNode(t.GetRoot())
	if err != nil {
		return err
	}
	clonedNode, err := tnode.updateRecord(record, t)
	if err == nil {
		t.Root = proto.Int64(clonedNode.GetId())
	}
	return err
}

// update node
func (n *TreeNode) updateRecord(record TreeLog, tree *Btree) (*TreeNode, error) {
	index := n.locate(record.Key)
	tnode, err := tree.getTreeNode(n.Childrens[index])
	if err != nil {
		return tnode, err
	}
	var nnode *TreeNode
	var clonedNode *TreeNode
	if tnode.GetNodeType() == isNode {
		clonedNode, err = tnode.updateRecord(record, tree)
		if err == nil {
			nnode = n.clone(tree)
			nnode.Childrens[index] = clonedNode.GetId()
		}
	} else {
		index--
		if index >= 0 {
			if bytes.Compare(n.Keys[index], record.Key) == 0 {
				nnode = n.clone(tree)
				nnode.Values[index] = record.Value
			}
		}
	}
	tree.nodes[nnode.GetId()], err = proto.Marshal(nnode)
	return nnode, err
}
