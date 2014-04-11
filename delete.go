package btree

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"sync/atomic"
)

func (t *Btree) dodelete(key []byte) error {
	tnode, err := t.getTreeNode(t.GetRoot())
	if err != nil {
		return err
	}
	clonedNode, _, err := tnode.deleteRecord(key, t)
	if err == nil {
		if len(clonedNode.GetKeys()) == 0 {
			if clonedNode.GetNodeType() == isNode {
				if len(clonedNode.GetChildrens()) > 0 {
					newroot, err := t.getTreeNode(clonedNode.Childrens[0])
					if err == nil {
						atomic.StoreInt32(clonedNode.IsDirt, 1)
						t.Nodes[clonedNode.GetId()], err = proto.Marshal(clonedNode)
						t.Root = proto.Int64(newroot.GetId())
					}
					return err
				}
			}
		}
		t.Root = proto.Int64(clonedNode.GetId())
	}
	return err
}

func (n *TreeNode) deleteRecord(key []byte, tree *Btree) (*TreeNode, []byte, error) {
	index := n.locate(key)
	var err error
	var nnode *TreeNode
	var newKey []byte
	var clonedNode *TreeNode
	if n.GetNodeType() == isNode {
		tnode, err := tree.getTreeNode(n.Childrens[index])
		clonedNode, newKey, err = tnode.deleteRecord(key, tree)
		if err == nil {
			nnode = n.clone(tree)
			nnode.Childrens[index] = clonedNode.GetId()
			tmpKey := newKey
			if len(newKey) > 0 {
				if nnode.replace(key, newKey) {
					newKey = []byte{}
				}
			}
			if index == 0 {
				index = 1
			}
			if len(nnode.Keys) > 0 {
				left := nnode.merge(tree, index-1)
				if index == 1 && len(tmpKey) == 0 {
					tt, _ := tree.getTreeNode(nnode.Childrens[0])
					if tt.GetNodeType() == isLeaf {
						if len(tt.Keys) > 0 {
							newKey = tt.Keys[0]
						}
					}
				}
				if left > 0 {
					nnode.Childrens[index-1] = left
				}
			}
		}
	}
	if n.GetNodeType() == isLeaf {
		index -= 1
		if index >= 0 && bytes.Compare(n.Keys[index], key) == 0 {
			nnode = n.clone(tree)
			nnode.Keys = append(nnode.Keys[:index], nnode.Keys[index+1:]...)
			nnode.Values = append(nnode.Values[:index], nnode.Values[index+1:]...)
			if index == 0 && len(nnode.Keys) > 0 {
				newKey = nnode.Keys[0]
			}
		} else {
			return nil, newKey, fmt.Errorf("delete failed")
		}
	}
	tree.Nodes[nnode.GetId()], err = proto.Marshal(nnode)
	return nnode, newKey, err
}

// replace delete key
func (n *TreeNode) replace(oldKey, newKey []byte) bool {
	index := n.locate(oldKey) - 1
	if index >= 0 {
		if bytes.Compare(n.Keys[index], oldKey) == 0 {
			n.Keys[index] = newKey
			return true
		}
	}
	return false
}
