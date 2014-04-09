package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
)

// Insert can insert record into a btree
func (t *Btree) insert(record TreeLog) error {
	tnode, err := t.getTreeNode(t.GetRoot())
	if err != nil && err.Error() == "no data" {
		nnode := t.newTreeNode()
		nnode.NodeType = proto.Int32(isLeaf)
		_, err = nnode.insertRecord(record, t)
		return err
	}
	if err != nil {
		return err
	} else {
		clonednode, err := tnode.insertRecord(record, t)
		if err == nil && len(clonednode.GetKeys()) > int(t.GetNodeMax()) {
			nnode := t.newTreeNode()
			nnode.NodeType = proto.Int32(isLeaf)
			key, left, right := clonednode.split(t)
			nnode.insertOnce(key, left, right, t)
			t.nodes[nnode.GetId()], err = proto.Marshal(nnode)
		}
		return err
	}
	return fmt.Errorf("bad insert")
}

// insert node
func (n *TreeNode) insertRecord(record TreeLog, tree *Btree) (*TreeNode, error) {
	var err error
	index := n.locate(record.Key)
	if n.GetNodeType() == isNode {
		tnode, err := tree.getTreeNode(n.Childrens[index])
		clonedTreeNode, err := tnode.insertRecord(record, tree)
		if err == nil {
			clonedNode := n.clone(tree)
			clonedNode.Childrens[index] = clonedTreeNode.GetId()
			if len(clonedTreeNode.GetKeys()) > int(tree.GetNodeMax()) {
				key, left, right := clonedTreeNode.split(tree)
				err = clonedNode.insertOnce(key, left, right, tree)
				if err != nil {
					return nil, err
				}
			}
			tree.nodes[clonedNode.GetId()], err = proto.Marshal(clonedNode)
			return clonedNode, err
		}
		return nil, err
	}
	if n.GetNodeType() == isNode {
		if index > 0 {
			if bytes.Compare(n.Keys[index-1], record.Key) == 0 {
				return nil, fmt.Errorf("key already inserted")
			}
		}
		var nnode *TreeNode
		if len(n.GetKeys()) == 0 {
			nnode = n
		} else {
			nnode = n.clone(tree)
		}
		nnode.Keys = append(nnode.Keys[:index],
			append([][]byte{record.Key}, nnode.Keys[index:]...)...)
		nnode.Values = append(nnode.Values[:index],
			append([][]byte{record.Value}, nnode.Values[index:]...)...)
		tree.nodes[nnode.GetId()], err = proto.Marshal(nnode)
		return nnode, err
	}
	return nil, fmt.Errorf("insert record failed")
}

// Insert key into tree node
func (n *TreeNode) insertOnce(key []byte, leftID int64, rightID int64, tree *Btree) error {
	var err error
	index := n.locate(key)
	if len(n.Keys) == 0 {
		n.Childrens = append([]int64{leftID}, rightID)
	} else {
		n.Childrens = append(n.Childrens[:index+1],
			append([]int64{rightID}, n.Childrens[index+1:]...)...)
	}
	n.Keys = append(n.Keys[:index], append([][]byte{key}, n.Keys[index:]...)...)
	tree.nodes[n.GetId()], err = proto.Marshal(n)
	return err
}
