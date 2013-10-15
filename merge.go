package btree

// merge leaf
func (n *Node) mergeLeaf(leftID int32, rightID int32, index int, tree *Btree) int32 {
	left := getLeaf(leftID, tree)
	right := getLeaf(rightID, tree)
	if (len(left.Values) + len(right.Values)) > int(tree.GetLeafMax()) {
		return -1
	}
	// clone left child
	leftClone := left.clone(tree).(*Leaf)
	id := *getTreeNodeId(leftClone)
	tree.nodes[id] = leftClone
	n.Childrens[index] = id
	// remove rightID
	if index == len(n.Keys) {
		n.Childrens = n.Childrens[:index]
		n.Keys = n.Keys[:index-1]
	} else {
		n.Childrens = append(n.Childrens[:index+1],
			n.Childrens[index+2:]...)
		n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
	}
	// add right to left
	leftClone.Values = append(leftClone.Values, right.Values...)
	leftClone.Keys = append(leftClone.Keys, right.Keys...)
	// cleanup old data
	markDup(leftID, tree)
	markDup(rightID, tree)
	return *leftClone.Id
}

// merge node
func (n *Node) mergeNode(leftID int32, rightID int32, index int, tree *Btree) int32 {
	left := getNode(leftID, tree)
	right := getNode(rightID, tree)
	if len(left.Keys)+len(right.Keys) > int(tree.GetNodeMax()) {
		return -1
	}
	// clone left node
	leftClone := left.clone(tree).(*Node)
	id := *getTreeNodeId(leftClone)
	tree.nodes[id] = leftClone
	n.Childrens[index] = id
	// merge key
	leftClone.Keys = append(leftClone.Keys,
		append([][]byte{n.Keys[index]},
			right.Keys...)...)
	// merge childrens
	leftClone.Childrens = append(leftClone.Childrens, right.Childrens...)
	// remove old key
	n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
	// remove old right node
	n.Childrens = append(n.Childrens[:index+1],
		n.Childrens[index+2:]...)
	// check size, spilt if over size
	if len(leftClone.Keys) > int(tree.GetNodeMax()) {
		key, left, right := leftClone.split(tree)
		n.insertOnce(key, left, right, tree)
	}
	// cleanup old
	markDup(leftID, tree)
	markDup(rightID, tree)
	return *leftClone.Id
}
