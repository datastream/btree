package btree

import (
	"fmt"
)

//debug func
func (this *Node) PrintChildrens() {
	for i := range this.Childrens {
		fmt.Println("Node", this.GetId(), "Child", this.Childrens[i])
	}
}

func (this *Node) PrintKeys() {
	for i := range this.Keys {
		fmt.Println("Node", this.GetId(), "Key", string(this.Keys[i]))
	}
}
func (this *Leaf) PrintKeys() {
	for i := range this.Keys {
		fmt.Println("Leaf", this.GetId(), "Key", string(this.Keys[i]))
	}
}

func (this *Btree) PrintInfo() {
	fmt.Println("Root", this.GetRoot())
	fmt.Println("IndexCursor", this.GetIndexCursor())
	fmt.Println("LeafCount", *this.LeafCount)
	fmt.Println("NodeCount", *this.NodeCount)
}

func (this *Btree) PrintTree() {
	fmt.Println("-----------Tree-------------")
	for i := 0; i < int(this.GetIndexCursor()); i++ {
		if node, ok := this.nodes[i].(*Node); ok {
			node.PrintKeys()
			node.PrintChildrens()
		}
		if leaf, ok := this.nodes[i].(*Leaf); ok {
			leaf.PrintKeys()
		}
		fmt.Println("AA")
	}
}
