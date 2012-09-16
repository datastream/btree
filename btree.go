package btree

import (
	"bytes"
)

/*
 * Insert
 */
func insert(treenode TreeNode, record *Record, tree *Btree) (rst, split bool, key []byte, left, right, refer int32) {
	var dup_id int32
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		rst = clonenode.insert(record, tree)
		if len(clonenode.Keys) > int(tree.GetNodeMax()) {
			key, left, right = clonenode.split(tree)
			if node.GetId() == tree.GetRoot() {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, left, right, tree)
				tree.Root = tnode.Id
			} else {
				split = true
			}
		}
		if rst {
			if node.GetId() == tree.GetRoot() {
				tree.Root = clonenode.Id
			}
			dup_id = clonenode.GetId()
			mark_dup(node.GetId(), tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst = cloneleaf.insert(record, tree)
		if len(cloneleaf.Values) > int(tree.GetLeafMax()) {
			key, left, right = cloneleaf.split(tree)
			if leaf.GetId() == tree.GetRoot() {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, left, right, tree)
				tree.Root = tnode.Id
			} else {
				split = true
			}
		}
		if rst {
			if leaf.GetId() == tree.GetRoot() {
				tree.Root = cloneleaf.Id
			}
			dup_id = cloneleaf.GetId()
			mark_dup(leaf.GetId(), tree)
		}
	}
	refer = dup_id
	return
}
func (this *Node) insert(record *Record, tree *Btree) bool {
	index := this.locate(record.Key)
	rst, split, key, left, right, refer := insert(tree.nodes[this.Childrens[index]], record, tree)
	if rst {
		this.Childrens[index] = refer
		if split {
			this.insert_once(key, left, right, tree)
		}
	} else {
		remove(this.GetId(), tree)
	}
	return rst
}
func (this *Leaf) insert(record *Record, tree *Btree) bool {
	index := this.locate(record.Key)
	if index > 0 {
		if bytes.Compare(this.Keys[index-1], record.Key) == 0 {
			remove(this.GetId(), tree)
			return false
		}
	}
	this.Keys = append(this.Keys[:index], append([][]byte{record.Key}, this.Keys[index:]...)...)
	this.Values = append(this.Values[:index], append([][]byte{record.Value}, this.Values[index:]...)...)
	return true
}

/*
 * Search
 */
func search(treenode TreeNode, key []byte, tree *Btree) []byte {
	if node, ok := treenode.(*Node); ok {
		return node.search(key, tree)
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.search(key, tree)
	}
	return nil

}
func (this *Node) search(key []byte, tree *Btree) []byte {
	index := this.locate(key)
	return search(tree.nodes[this.Childrens[index]], key, tree)
}
func (this *Leaf) search(key []byte, tree *Btree) []byte {
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], key) == 0 {
			return this.Values[index]
		}
	}
	return nil
}

/*
 * Delete
 */
func delete(treenode TreeNode, key []byte, tree *Btree) (rst bool, refer int32) {
	var dup_id int32
	rst = false
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		if node.GetId() == tree.GetRoot() {
			tree.cloneroot = clonenode.GetId()
		}
		if rst = clonenode.delete(key, tree); rst {
			if node.GetId() == tree.GetRoot() {
				if len(clonenode.Keys) == 0 {
					tree.Root = get_id(clonenode.Childrens[0], tree)
					remove(tree.cloneroot, tree)
				} else {
					tree.Root = clonenode.Id
				}
			}
			dup_id = clonenode.GetId()
			mark_dup(node.GetId(), tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		if rst = cloneleaf.delete(key, tree); rst {
			if leaf.GetId() == tree.GetRoot() {
				tree.Root = cloneleaf.Id
			}
			dup_id = cloneleaf.GetId()
			mark_dup(leaf.GetId(), tree)
		}
	}
	refer = dup_id
	return
}
func (this *Node) delete(key []byte, tree *Btree) bool {
	index := this.locate(key)
	rst, refer := delete(tree.nodes[this.Childrens[index]], key, tree)
	if rst {
		this.Childrens[index] = refer
		if index == 0 {
			index = 1
		}
		if len(this.Keys) > 0 {
			if get_node(this.Childrens[0], tree) != nil {
				this.mergenode(this.Childrens[index-1], this.Childrens[index], index-1, tree)
			} else {
				removed_key := this.Keys[0]
				this.mergeleaf(this.Childrens[index-1], this.Childrens[index], index-1, tree)
				if index == 1 {
					replace(key, removed_key, tree.cloneroot, tree)
				}
			}
		}
		return true
	}
	remove(this.GetId(), tree)
	return false
}
func (this *Leaf) delete(key []byte, tree *Btree) bool {
	var deleted bool
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], key) == 0 {
			deleted = true
		}
	}
	if deleted {
		this.Keys = append(this.Keys[:index], this.Keys[index+1:]...)
		this.Values = append(this.Values[:index], this.Values[index+1:]...)
		if index == 0 && len(this.Keys) > 0 {
			if tree.cloneroot != this.GetId() {
				replace(key, this.Keys[0], tree.cloneroot, tree)
			}
		}
		return true
	}
	remove(this.GetId(), tree)
	return false
}

/*
 * Update
 */
func update(treenode TreeNode, record *Record, tree *Btree) (rst bool, refer int32) {
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		rst = clonenode.update(record, tree)
		if rst {
			refer = clonenode.GetId()
			if tree.GetRoot() == node.GetId() {
				tree.Root = clonenode.Id
			}
			mark_dup(node.GetId(), tree)
		}
		return
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst = cloneleaf.update(record, tree)
		if rst {
			refer = cloneleaf.GetId()
			if tree.GetRoot() == leaf.GetId() {
				tree.Root = cloneleaf.Id
			}
			mark_dup(leaf.GetId(), tree)
		}
		return
	}
	return
}
func (this *Node) update(record *Record, tree *Btree) bool {
	index := this.locate(record.Key)
	stat, clone := update(tree.nodes[this.Childrens[index]], record, tree)
	if stat {
		this.Childrens[index] = clone
	} else {
		remove(this.GetId(), tree)
	}
	return stat
}

func (this *Leaf) update(record *Record, tree *Btree) bool {
	index := this.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], record.Key) == 0 {
			this.Values[index] = record.Value
			return true
		}
	}
	remove(this.GetId(), tree)
	return false
}

/*
 * Split
 */
func (this *Leaf) split(tree *Btree) (key []byte, left, right int32) {
	newleaf := get_leaf(tree.newleaf(), tree)
	mid := tree.GetLeafMax() / 2
	newleaf.Values = make([][]byte, len(this.Values[mid:]))
	newleaf.Keys = make([][]byte, len(this.Keys[mid:]))
	copy(newleaf.Values, this.Values[mid:])
	copy(newleaf.Keys, this.Keys[mid:])
	this.Values = this.Values[:mid]
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

/*
 * insert key into tree node
 */
func (this *Node) insert_once(key []byte, left_id int32, right_id int32, tree *Btree) {
	index := this.locate(key)
	if len(this.Keys) == 0 {
		this.Childrens = append([]int32{left_id}, right_id)
	} else {
		this.Childrens = append(this.Childrens[:index+1], append([]int32{right_id}, this.Childrens[index+1:]...)...)
	}
	this.Keys = append(this.Keys[:index], append([][]byte{key}, this.Keys[index:]...)...)
}

/*
 * Replace key in node
 */
func replace(oldkey []byte, newkey []byte, id int32, tree *Btree) {
	node := get_node(id, tree)
	if node != nil {
		index := node.locate(oldkey) - 1
		if index >= 0 {
			if bytes.Compare(node.Keys[index], oldkey) == 0 {
				node.Keys[index] = newkey
				return
			} else {
				replace(oldkey, newkey, node.Childrens[index+1], tree)
			}
		}
	}
}

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

func remove(index int32, tree *Btree) {
	if node, ok := tree.nodes[index].(*Node); ok {
		tree.FreeList = append(tree.FreeList, node.GetId())
		*tree.NodeCount--
	}
	if leaf, ok := tree.nodes[index].(*Leaf); ok {
		tree.FreeList = append(tree.FreeList, leaf.GetId())
		*tree.LeafCount--
	}
	tree.nodes[index] = nil
}
func mark_dup(index int32, tree *Btree) {
	if tree.stat == 1 {
		if _, ok := tree.nodes[index].(*Node); ok {
			*tree.NodeCount--
		}
		if _, ok := tree.nodes[index].(*Leaf); ok {
			*tree.LeafCount--
		}
		tree.dupnodelist = append(tree.dupnodelist, index)
	} else {
		remove(index, tree)
	}
}
func get_node(id int32, tree *Btree) *Node {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node
	}
	return nil
}
func get_leaf(id int32, tree *Btree) *Leaf {
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf
	}
	return nil
}
func get_id(id int32, tree *Btree) *int32 {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node.Id
	}
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf.Id
	}
	return nil
}
func (this *Node) locate(key []byte) int {
	i := 0
	size := len(this.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(this.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}
func (this *Leaf) locate(key []byte) int {
	i := 0
	size := len(this.Keys)
	for {
		mid := (i + size) / 2
		if i == size {
			break
		}
		if bytes.Compare(this.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}
func (this *Btree) clonenode(node *Node) *Node {
	newnode := get_node(this.newnode(), this)
	newnode.Keys = make([][]byte, len(node.Keys))
	copy(newnode.Keys, node.Keys)
	newnode.Childrens = make([]int32, len(node.Childrens))
	copy(newnode.Childrens, node.Childrens)
	return newnode
}
func (this *Btree) cloneleaf(leaf *Leaf) *Leaf {
	newleaf := get_leaf(this.newleaf(), this)
	newleaf.Keys = make([][]byte, len(leaf.Keys))
	copy(newleaf.Keys, leaf.Keys)
	newleaf.Values = make([][]byte, len(leaf.Values))
	copy(newleaf.Values, leaf.Values)
	return newleaf
}
func (this *Btree) gc() {
	for {
		if len(this.dupnodelist) > 0 && this.stat == 0 {
			id := this.dupnodelist[len(this.dupnodelist)-1]
			this.dupnodelist = this.dupnodelist[:len(this.dupnodelist)-1]
			remove(id, this)
		} else {
			break
		}
	}
}
func encodefixed32(x uint64) []byte {
	var p []byte
	p = append(p,
		uint8(x),
		uint8(x>>8),
		uint8(x>>16),
		uint8(x>>24))
	return p
}
func decodefixed32(num []byte) (x uint64) {
	x = uint64(num[0])
	x |= uint64(num[1]) << 8
	x |= uint64(num[2]) << 16
	x |= uint64(num[3]) << 24
	return
}
