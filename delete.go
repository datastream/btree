package btree

import (
	"bytes"
)

// delete in cloned node
func (n *Node) deleteRecord(key []byte, tree *Btree) (bool, TreeNode, []byte) {
	index := n.locate(key)
	if rst, clonedTreeNode, newKey := tree.nodes[n.Childrens[index]].deleteRecord(key, tree); rst {
		clonedNode, _ := n.clone(tree).(*Node)
		tree.nodes[clonedNode.GetId()] = clonedNode
		clonedNode.Childrens[index] = *getTreeNodeID(clonedTreeNode)
		if n.GetId() == tree.GetRoot() {
			tree.cloneroot = clonedNode.GetId()
		}
		tmpKey := newKey
		if newKey != nil {
			if clonedNode.replace(key, newKey) {
				newKey = nil
			}
		}
		if index == 0 {
			index = 1
		}
		if len(clonedNode.Keys) > 0 {
			var left int32
			if getLeaf(clonedNode.Childrens[index-1], tree) != nil {
				left = clonedNode.mergeLeaf(
					clonedNode.Childrens[index-1],
					clonedNode.Childrens[index],
					index-1,
					tree)
				if index == 1 && tmpKey == nil {
					leaf := getLeaf(
						clonedNode.Childrens[0],
						tree)
					if leaf != nil && len(leaf.Keys) > 0 {
						newKey = leaf.Keys[0]
					}
				}
			} else {
				left = clonedNode.mergeNode(
					clonedNode.Childrens[index-1],
					clonedNode.Childrens[index],
					index-1,
					tree)
			}
			if left > 0 {
				clonedNode.Childrens[index-1] = left
			}
		}
		markDup(*n.Id, tree)
		return true, clonedNode, newKey
	}
	return false, nil, nil
}

//delete record in a lead
//first return deleted or not
//second return cloneTreeNode
func (l *Leaf) deleteRecord(key []byte, tree *Btree) (bool, TreeNode, []byte) {
	deleted := false
	index := l.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(l.Keys[index], key) == 0 {
			deleted = true
		}
	}
	if deleted {
		clonedLeaf, _ := l.clone(tree).(*Leaf)
		clonedLeaf.Keys = append(clonedLeaf.Keys[:index],
			clonedLeaf.Keys[index+1:]...)
		clonedLeaf.Values = append(clonedLeaf.Values[:index],
			clonedLeaf.Values[index+1:]...)
		if l.GetId() == tree.GetRoot() {
			tree.cloneroot = clonedLeaf.GetId()
		}
		tree.nodes[clonedLeaf.GetId()] = clonedLeaf
		markDup(*l.Id, tree)
		if index == 0 && len(clonedLeaf.Keys) > 0 {
			return true, clonedLeaf, clonedLeaf.Keys[0]
		} else {
			return true, clonedLeaf, nil
		}
	}
	return false, nil, nil
}

// replace delete key
func (n *Node) replace(oldKey, newKey []byte) bool {
	index := n.locate(oldKey) - 1
	if index >= 0 {
		if bytes.Compare(n.Keys[index], oldKey) == 0 {
			n.Keys[index] = newKey
			return true
		}
	}
	return false
}
