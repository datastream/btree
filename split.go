package btree

//split leaf
func (this *Leaf) split(tree *Btree) (key []byte, left, right int32) {
	new_leaf := tree.newleaf()
	mid := tree.GetLeafMax() / 2
	new_leaf.Values = make([][]byte, len(this.Values[mid:]))
	copy(new_leaf.Values, this.Values[mid:])
	new_leaf.Keys = make([][]byte, len(this.Keys[mid:]))
	this.Values = this.Values[:mid]
	copy(new_leaf.Keys, this.Keys[mid:])
	this.Keys = this.Keys[:mid]
	left = this.GetId()
	right = new_leaf.GetId()
	key = new_leaf.Keys[0]
	tree.nodes[new_leaf.GetId()] = new_leaf
	return
}

//split node
func (this *Node) split(tree *Btree) (key []byte, left, right int32) {
	new_node := tree.newnode()
	mid := tree.GetNodeMax() / 2
	key = this.Keys[mid]
	new_node.Keys = make([][]byte, len(this.Keys[mid+1:]))
	copy(new_node.Keys, this.Keys[mid+1:])
	this.Keys = this.Keys[:mid]
	new_node.Childrens = make([]int32, len(this.Childrens[mid+1:]))
	copy(new_node.Childrens, this.Childrens[mid+1:])
	this.Childrens = this.Childrens[:mid+1]
	left = this.GetId()
	right = new_node.GetId()
	tree.nodes[new_node.GetId()] = new_node
	return
}
