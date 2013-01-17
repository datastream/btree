package btree

/*
 * merge leaf/node
 */
func (this *Node) mergeleaf(left_id int32, right_id int32, index int, tree *Btree) {
	left := get_leaf(left_id, tree)
	right := get_leaf(right_id, tree)
	if (len(left.Values) + len(right.Values)) > int(tree.GetLeafMax()) {
		return
	}
	if index == len(this.Keys) {
		this.Childrens = this.Childrens[:index]
		this.Keys = this.Keys[:index-1]
	} else {
		this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
		this.Keys = append(this.Keys[:index], this.Keys[index+1:]...)
	}
	left.Values = append(left.Values, right.Values...)
	left.Keys = append(left.Keys, right.Keys...)
	mark_dup(right.GetId(), tree)
}
func (this *Node) mergenode(left_id int32, right_id int32, index int, tree *Btree) {
	left_node := get_node(left_id, tree)
	right_node := get_node(right_id, tree)
	if len(left_node.Keys)+len(right_node.Keys) > int(tree.GetNodeMax()) {
		return
	}
	left_node.Keys = append(left_node.Keys, append([][]byte{this.Keys[index]}, right_node.Keys...)...)
	left_node.Childrens = append(left_node.Childrens, right_node.Childrens...)
	this.Keys = append(this.Keys[:index], this.Keys[index+1:]...)
	this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
	mark_dup(*right_node.Id, tree)
	if len(left_node.Keys) > int(tree.GetNodeMax()) {
		key, left, right := left_node.split(tree)
		this.insert_once(key, left, right, tree)
	}
}
