package btree

import (
	"fmt"
)

//debug func

func (n *TreeNode) printChildrens() {
	for i := range n.Childrens {
		fmt.Println("Node", n.GetId(), "Child", n.Childrens[i])
	}
}

func (n *TreeNode) printKeys() {
	for i := range n.Keys {
		fmt.Println("TreeNode", n.GetId(), "Key", string(n.Keys[i]))
	}
}

// PrintInfo print some basic btree info
func (t *Btree) PrintInfo() {
	fmt.Println("Root", t.GetRoot())
	fmt.Println("IndexCursor", t.GetIndexCursor())
}

// PrintTree print all btree's leafs/nodes
func (t *Btree) PrintTree() {
	fmt.Println("-----------Tree-------------")
	for i := 0; i < int(t.GetIndexCursor()); i++ {
		if node, err := t.getTreeNode(int64(i)); err == nil {
			node.printKeys()
			node.printChildrens()
			node.printKeys()
		}
		fmt.Println("AA")
	}
}
