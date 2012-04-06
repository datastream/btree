package btree

import (
	"sync"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
)

const  NODEIDBASE = 1<<18

type Btree struct {
	info *BtreeMetaData
	nodes []TreeNode
	sync.RWMutex
	stat int
	cond *sync.Cond
}
type  Leaf struct {
	LeafMetaData
	sync.RWMutex
}
type Node struct {
	NodeMetaData
	sync.RWMutex
}
type TreeNode interface {
	insert(record *RecordMetaData, tree *Btree) bool
	delete(key []byte, tree *Btree) bool
	update(recode *RecordMetaData, tree *Btree) bool
	search(key []byte, tree *Btree) []byte
}

func NewBtree() *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, 1<<20) //26 -> 1G mem
	tree.stat = 0
	tree.cond = sync.NewCond(tree)
	tree.info = &BtreeMetaData{
	Size: proto.Uint32(32),
	LeafMax:  proto.Int32(NODEIDBASE-1),
	NodeMax: proto.Int32(1<<18),
	LeafCount: proto.Int32(0),
	NodeCount:  proto.Int32(0),
	LastLeaf: proto.Int32(-1),
	LastNode: proto.Int32(NODEIDBASE-1),
	FirstLeaf: proto.Int32(0),
	}
	tree.info.Root =  proto.Int32(tree.newleaf())
	return tree
}

func NewBtreeSize(size uint32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, 1<<20)
	tree.stat = 0
	tree.cond = sync.NewCond(tree)
	tree.info = &BtreeMetaData{
	Size: proto.Uint32(size),
	LeafMax:  proto.Int32(NODEIDBASE-1),
	NodeMax: proto.Int32(1<<19),
	LeafCount: proto.Int32(0),
	NodeCount:  proto.Int32(0),
	LastLeaf: proto.Int32(-1),
	LastNode: proto.Int32(NODEIDBASE-1),
	FirstLeaf: proto.Int32(0),
	}
	tree.info.Root =  proto.Int32(tree.newleaf())
	return tree
}

func (this *Btree) Insert(record *RecordMetaData, rst chan bool) {
	this.Lock()
	if this.stat > 0 {
		this.cond.Wait()
	}
	if this.free_node_count() < 50 || this.free_leaf_count() < 50 {
		rst <- false
		return
	}
	this.Unlock()
	rst <- insert(this.nodes[*this.info.Root], record, this)
}

func (this *Btree) Delete(key []byte, rst chan bool) {
	this.Lock()
	if this.stat > 0 {
		this.cond.Wait()
	}
	this.Unlock()
	rst <- delete(this.nodes[*this.info.Root], key, this)
}

func (this *Btree) Search(key []byte, rst chan []byte) {
	this.Lock()
	if this.stat > 0 {
		this.cond.Wait()
	}
	this.Unlock()
	rst <- search(this.nodes[*this.info.Root], key, this)
}

func (this *Btree) Update(record *RecordMetaData, rst chan bool) {
	this.Lock()
	if this.stat > 0 {
		this.cond.Wait()
	}
	this.Unlock()
	rst <- update(this.nodes[*this.info.Root], record, this)
}
/*
 * alloc leaf/node
 */
func (this *Btree) newleaf() int32 {
	this.Lock()
	defer this.Unlock()
	var id int32
	*this.info.LastLeaf ++
	*this.info.LeafCount ++
	leaf := new(Leaf)
	leaf.Removed = proto.Bool(false)
	if len(this.info.FreeLeafList) > 0 {
		id = this.info.FreeLeafList[len(this.info.FreeLeafList)-1]
		this.info.FreeLeafList = this.info.FreeLeafList[:len(this.info.FreeLeafList)-1]
	} else {
		id = *this.info.LastLeaf
	}
	leaf.Id = proto.Int32(id)
	this.nodes[*leaf.Id] = leaf
	return id
}
func (this *Btree) newnode() int32 {
	this.Lock()
	defer this.Unlock()
	var id int32
	*this.info.LastNode ++
	*this.info.NodeCount ++
	node := new(Node)
	node.Removed = proto.Bool(false)
	if len(this.info.FreeNodeList) > 0 {
		id = this.info.FreeNodeList[len(this.info.FreeNodeList)-1]
		this.info.FreeNodeList = this.info.FreeNodeList[:len(this.info.FreeNodeList)-1]
	} else {
		id = *this.info.LastNode
	}
	node.Id = proto.Int32(id)
	this.nodes[*node.Id] = node
	return id
}
/*
 * Insert
 */
func insert(treenode TreeNode, record *RecordMetaData, tree *Btree) bool {
	if node, ok := treenode.(*Node); ok {
		return node.insert(record, tree)

	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.insert(record, tree)
	}
	return false
}
func (this *Node) insert(record *RecordMetaData, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key)
	return insert(tree.nodes[this.Childrens[index]], record, tree)
}
func (this *Leaf) insert(record *RecordMetaData, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key)
	if index > 0 {
		if bytes.Compare(this.Records[index-1].Key, record.Key) == 0 {
			return false
		}
	}
	this.Records = append(this.Records[:index], append([]*RecordMetaData{record}, this.Records[index:]...)...)
	if uint32(len(this.Records)) > *tree.info.Size {
		this.split(tree)
	}
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
	this.RLock()
	defer this.RUnlock()
	index := this.locate(key)
	return search(tree.nodes[this.Childrens[index]], key, tree)
}
func (this *Leaf) search(key []byte, tree *Btree) []byte {
	this.RLock()
	defer this.RUnlock()
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Records[index].Key, key) == 0 {
			return this.Records[index].Value
		}
	}
	return nil
}
/*
 * Delete
 */
func delete(treenode TreeNode, key []byte, tree *Btree) bool {
	if node, ok := treenode.(*Node); ok {
		return node.delete(key, tree)
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.delete(key, tree)
	}
	return false
}
func (this *Node) delete(key []byte, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(key)
	return delete(tree.nodes[this.Childrens[index]], key, tree)
}
func (this *Leaf) delete(key []byte, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	var deleted bool
	index := this.locate(key) -1
	if index >= 0 {
		if bytes.Compare(this.Records[index].Key, key) == 0 {
			deleted = true
		}
	}
	if deleted {
		this.Records = append(this.Records[:index],this.Records[index+1:]...)
		if index == 0 && len(this.Records) > 0 {
			if tree.info.Root != this.Id {
				replace(key, this.Records[0].Key, *this.Father, tree)
			}
		}
		if this.Id != tree.info.Root {
			node := tree.nodes[*this.Father]
			if n, ok := node.(*Node); ok {
				merge(key, n, tree)
			}
		}
		return true
	}
	return false
}
/*
 * Update
 */
func update(treenode TreeNode, record *RecordMetaData, tree *Btree) bool {
	if node, ok := treenode.(*Node); ok {
		return node.update(record, tree)
	}
	if leaf, ok := treenode.(*Leaf); ok {
		return leaf.update(record, tree)
	}
	return false
}
func (this *Node) update(record *RecordMetaData, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key)
	return tree.nodes[this.Childrens[index]].update(record, tree)
}

func (this *Leaf) update(record *RecordMetaData, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(this.Records[index].Key, record.Key) == 0 {
			this.Records[index].Value = record.Value
			return true
		}
	}
	return false
}
/*
 * Split
 */
func (this *Leaf) split(tree *Btree) {
	newleaf := get_leaf(tree.newleaf(), tree)
	newleaf.Records = make([]*RecordMetaData, len(this.Records[*tree.info.Size/2:]))
	copy(newleaf.Records, this.Records[*tree.info.Size/2:])
	this.Records = this.Records[:*tree.info.Size/2]
	this.Next = newleaf.Id
	newleaf.Prev = this.Id
	if *tree.info.NodeCount != 0 {
		tnode := get_node(*this.Father, tree)
		newleaf.Father = this.Father
		tnode.insert_once(newleaf.Records[0].Key, *this.Id, *newleaf.Id, tree)
	} else {
		tnode := get_node(tree.newnode(), tree)
		tnode.insert_once(newleaf.Records[0].Key, *this.Id, *newleaf.Id, tree)
		this.Father = tnode.Id
		newleaf.Father = this.Father
		tree.Lock()
		tree.info.Root = tnode.Id
		tree.Unlock()
	}
}
func (this *Node) split(tree *Btree) {
	newnode := get_node(tree.newnode(), tree)
	key := this.Keys[*tree.info.Size/2]
	newnode.Keys = make([][]byte, len(this.Keys[*tree.info.Size/2+1:]))
	copy(newnode.Keys, this.Keys[*tree.info.Size/2+1:])
	this.Keys = this.Keys[:*tree.info.Size/2]
	newnode.Childrens = make([]int32, len(this.Childrens[*tree.info.Size/2+1:]))
	copy(newnode.Childrens, this.Childrens[*tree.info.Size/2+1:])
	this.Childrens = this.Childrens[:*tree.info.Size/2+1]
	for l := 0; l < len(newnode.Childrens); l++ {
		set_father(tree.nodes[newnode.Childrens[l]], newnode.Id)
	}
	if this.Id == tree.info.Root {
		tnode := get_node(tree.newnode(), tree)
		this.Father = tnode.Id
		newnode.Father = this.Father
		tnode.insert_once(key, *this.Id, *newnode.Id, tree)
		tree.Lock()
		tree.info.Root = tnode.Id
		tree.Unlock()
	} else {
		newnode.Father = this.Father
		tnode := get_node(*this.Father, tree)
		tnode.insert_once(key, *this.Id, *newnode.Id, tree)
	}

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
	if len(this.Keys) > int(*tree.info.Size) {
		this.split(tree)
	}
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
			}
		}
		if tree.info.Root != node.Id {
			replace(oldkey, newkey, *node.Father, tree)
		}
	}
}
/*
 * merge leaf/node
 */
func merge(key []byte, node *Node, tree *Btree) {
	index := node.locate(key)
	if index == 0 {
		index = 1
	}
	if get_node(node.Childrens[0], tree) != nil {
		node.mergenode(node.Childrens[index-1], node.Childrens[index], index-1, tree)
	} else {
		removed_key := node.Keys[0]
		node.mergeleaf(node.Childrens[index-1], node.Childrens[index], index-1, tree)
		if proto.GetBool(node.Removed) == false {
			replace(key, removed_key, *node.Id, tree)
		}
	}
}

func (this *Node) mergeleaf(left_id int32, right_id int32, index int, tree *Btree) {
	left := get_leaf(left_id, tree)
	right := get_leaf(right_id, tree)
	if (len(left.Records) + len(right.Records)) > int(*tree.info.Size) {
		return
	}
	if index == len(this.Keys) {
		this.Childrens = this.Childrens[:index]
		this.Keys = this.Keys[:index-1]
	} else {
		this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
		this.Keys = append(this.Keys[:index],this.Keys[index+1:]...)
	}
	left.Records = append(left.Records, right.Records...)
	right.Records = right.Records[:0]
	left.Next = right.Next
	if right.Next != nil {
		nextleaf := get_leaf(*right.Next, tree)
		nextleaf.Prev = left.Id
	}
	tree.Lock()
	remove(tree.nodes[*right.Id], tree)
	tree.Unlock()
	if this.Id != tree.info.Root {
		node := get_node(*this.Father, tree)
		merge(left.Records[0].Key, node, tree)
	} else {
		tree.Lock()
		if len(this.Keys) == 0 {
			remove(tree.nodes[*tree.info.Root], tree)
			tree.info.Root = left.Id
		}
		tree.Unlock()
	}
}
func (this *Node) mergenode(left_id int32, right_id int32, index int, tree *Btree) {
	left := get_node(left_id, tree)
	right := get_node(right_id, tree)
	if len(left.Keys) + len(right.Keys) >  int(*tree.info.Size) {
		return
	}
	key := this.Keys[0]
	for l := 0; l < len(right.Childrens); l++ {
		set_father(tree.nodes[right.Childrens[l]], left.Id)
	}
	left.Keys = append(left.Keys, append([][]byte{this.Keys[index]}, right.Keys...)...)
	left.Childrens = append(left.Childrens, right.Childrens...)
	right.Keys = right.Keys[:0]
	right.Childrens = right.Childrens[:0]
	this.Keys = append(this.Keys[:index],this.Keys[index+1:]...)
	this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
	tree.Lock()
	remove(tree.nodes[*right.Id], tree)
	tree.Unlock()
	if len(left.Keys) > int(*tree.info.Size) {
		left.split(tree)
	} else {
		if this.Id != tree.info.Root {
			node := get_node(*this.Father, tree)
			merge(key, node, tree)
		} else {
			tree.Lock()
			if len(this.Keys) == 0 {
				remove(tree.nodes[*tree.info.Root], tree)
				tree.info.Root = left.Id
			}
			tree.Unlock()
		}
	}
}

func remove(treenode TreeNode, tree *Btree) {
	if node, ok := treenode.(*Node); ok {
		tree.info.FreeNodeList = append(tree.info.FreeNodeList, *node.Id)
		node.Removed = proto.Bool(true)
		*tree.info.NodeCount --
	}
	if leaf, ok := treenode.(*Leaf); ok {
		tree.info.FreeLeafList = append(tree.info.FreeLeafList, *leaf.Id)
		leaf.Removed = proto.Bool(true)
		*tree.info.LeafCount --
	}
}
func get_father(treenode TreeNode) int32 {
	var id int32
	if node, ok := treenode.(*Node); ok {
		id = *node.Father
	}
	if leaf, ok := treenode.(*Leaf); ok {
		id = *leaf.Father
	}
	return id
}
func set_father(treenode TreeNode, id *int32) {
	if node, ok := treenode.(*Node); ok {
		node.Father = id
	}
	if leaf, ok := treenode.(*Leaf); ok {
		leaf.Father = id
	}
}
func get_node(id int32, tree *Btree) (*Node) {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node
	}
	return nil
}
func get_leaf(id int32, tree *Btree) (*Leaf) {
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf
	}
	return nil
}
func (this *Node) locate(key []byte) (int) {
	i := 0
	size := len(this.Keys)
	for {
		mid := (i+size)/2
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
func (this *Leaf) locate(key []byte) (int) {
	i := 0
	size := len(this.Records)
	for {
		mid := (i+size)/2
		if i == size {
			break
		}
		if bytes.Compare(this.Records[mid].Key, key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}
func (this *Btree)free_node_count() int32 {
	return *this.info.NodeMax - *this.info.NodeCount
}
func (this *Btree)free_leaf_count() int32 {
	return *this.info.LeafMax - *this.info.LeafCount
}
