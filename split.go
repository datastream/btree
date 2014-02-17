package btree

//split leaf
func (l *Leaf) split(tree *Btree) (key []byte, left, right int64) {
	nleaf := tree.newLeaf()
	mid := tree.GetLeafMax() / 2
	nleaf.Values = make([][]byte, len(l.Values[mid:]))
	copy(nleaf.Values, l.Values[mid:])
	nleaf.Keys = make([][]byte, len(l.Keys[mid:]))
	l.Values = l.Values[:mid]
	copy(nleaf.Keys, l.Keys[mid:])
	l.Keys = l.Keys[:mid]
	left = l.GetId()
	right = nleaf.GetId()
	key = nleaf.Keys[0]
	return
}

//split node
func (n *Node) split(tree *Btree) (key []byte, left, right int64) {
	nnode := tree.newNode()
	mid := tree.GetNodeMax() / 2
	key = n.Keys[mid]
	nnode.Keys = make([][]byte, len(n.Keys[mid+1:]))
	copy(nnode.Keys, n.Keys[mid+1:])
	n.Keys = n.Keys[:mid]
	nnode.Childrens = make([]int64, len(n.Childrens[mid+1:]))
	copy(nnode.Childrens, n.Childrens[mid+1:])
	n.Childrens = n.Childrens[:mid+1]
	left = n.GetId()
	right = nnode.GetId()
	return
}
