package btree

/*
 * Split
 */
func (this *Leaf) split(tree *Btree) (key []byte, left, right int32) {
	newleaf := get_leaf(tree.newleaf(), tree)
	mid := tree.GetLeafMax() / 2
	newleaf.Values = make([][]byte, len(this.Values[mid:]))
	copy(newleaf.Values, this.Values[mid:])
	newleaf.Keys = make([][]byte, len(this.Keys[mid:]))
	this.Values = this.Values[:mid]
	copy(newleaf.Keys, this.Keys[mid:])
	this.Keys = this.Keys[:mid]
	left = this.GetId()
	right = newleaf.GetId()
	key = newleaf.Keys[0]
	return
}
func (this *Node) split(tree *Btree) (key []byte, left, right int32) {
	newnode := get_node(tree.newnode(), tree)
	mid := tree.GetNodeMax() / 2
	key = this.Keys[mid]
	newnode.Keys = make([][]byte, len(this.Keys[mid+1:]))
	copy(newnode.Keys, this.Keys[mid+1:])
	this.Keys = this.Keys[:mid]
	newnode.Childrens = make([]int32, len(this.Childrens[mid+1:]))
	copy(newnode.Childrens, this.Childrens[mid+1:])
	this.Childrens = this.Childrens[:mid+1]
	left = this.GetId()
	right = newnode.GetId()
	return
}
