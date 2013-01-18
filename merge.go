package btree

/*
 * merge leaf/node
 */
func (this *Node) merge_leaf(left_id int32, right_id int32, index int, tree *Btree) int32 {
	left := get_leaf(left_id, tree)
	right := get_leaf(right_id, tree)
	if (len(left.Values) + len(right.Values)) > int(tree.GetLeafMax()) {
		return -1
	}
	left_clone := left.clone(tree).(*Leaf)
	id := *get_treenode_id(left_clone)
	tree.nodes[id] = left_clone
	this.Childrens[index] = id
	if index == len(this.Keys) {
		this.Childrens = this.Childrens[:index]
		this.Keys = this.Keys[:index-1]
	} else {
		this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
		this.Keys = append(this.Keys[:index], this.Keys[index+1:]...)
	}
	left_clone.Values = append(left_clone.Values, right.Values...)
	left_clone.Keys = append(left_clone.Keys, right.Keys...)
	mark_dup(left_id, tree)
	mark_dup(right_id, tree)
	return *left_clone.Id
}

func (this *Node) merge_node(left_id int32, right_id int32, index int, tree *Btree) int32 {
	left := get_node(left_id, tree)
	right := get_node(right_id, tree)
	if len(left.Keys)+len(right.Keys) > int(tree.GetNodeMax()) {
		return -1
	}
	left_clone := left.clone(tree).(*Node)
	id := *get_treenode_id(left_clone)
	tree.nodes[id] = left_clone
	this.Childrens[index] = id
	left_clone.Keys = append(left_clone.Keys, append([][]byte{this.Keys[index]}, right.Keys...)...)
	left_clone.Childrens = append(left_clone.Childrens, right.Childrens...)
	this.Keys = append(this.Keys[:index], this.Keys[index+1:]...)
	this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
	if len(left_clone.Keys) > int(tree.GetNodeMax()) {
		key, left, right := left_clone.split(tree)
		this.insert_once(key, left, right, tree)
	}
	mark_dup(left_id, tree)
	mark_dup(right_id, tree)
	return *left_clone.Id
}
