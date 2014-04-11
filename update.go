package btree

import (
	"bytes"
	"github.com/golang/protobuf/proto"
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
	var nnode *TreeNode
	var clonedNode *TreeNode
	var err error
	if n.GetNodeType() == isNode {
		tnode, err := tree.getTreeNode(n.Childrens[index])
		if err != nil {
			return tnode, err
		}
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
	tree.Nodes[nnode.GetId()], err = proto.Marshal(nnode)
	return nnode, err
}
