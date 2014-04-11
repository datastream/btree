package btree

import (
	"fmt"
)

//debug func

func (n *TreeNode) printNode() {
	if n.GetNodeType() == isLeaf {
		fmt.Println("---LeafID---", n.GetId())
		fmt.Println("Key", toArray(n.GetKeys()))
		fmt.Println("values", toArray(n.GetValues()))
	} else {
		fmt.Println("---NodeID---", n.GetId())
		fmt.Println("Key", toArray(n.GetKeys()))
		fmt.Println("Childrens", n.GetChildrens())
	}
}

func toArray(data [][]byte) []string {
	var rst []string
	for _, v := range data {
		rst = append(rst, string(v))
	}
	return rst
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
			if node.GetIsDirt() == 0 {
				node.printNode()
				fmt.Println("--------")
			}
		}
	}
}
