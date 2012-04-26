package btree

import (
	"sync"
	"bytes"
	"fmt"
	"code.google.com/p/goprotobuf/proto"
)

const  NODEIDBASE = 1<<18

type Btree struct {
	info *BtreeMetaData
	nodes []TreeNode
	sync.RWMutex
	stat int
	cond *sync.Cond
	cloneroot *int32
	snaproot []int32
	current_version int32
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
	tree.current_version = 0
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
	tree.info.Version = proto.Int32(0)
	tree.info.Root =  proto.Int32(tree.newleaf())
	fmt.Println("start tree")
	return tree
}

func NewBtreeSize(size uint32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, 1<<20)
	tree.stat = 0
	tree.current_version = 0
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
	tree.info.Version = proto.Int32(0)
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
	stat, _, _, _, _, _ := insert(this.nodes[*this.info.Root], record, this)
	rst <- stat
}

func (this *Btree) Delete(key []byte, rst chan bool) {
	this.Lock()
	if this.stat > 0 {
		this.cond.Wait()
	}
	this.Unlock()
	stat, _ :=  delete(this.nodes[*this.info.Root], key, this)
	rst <- stat
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
	stat, _ := update(this.nodes[*this.info.Root], record, this)
	rst <- stat
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
	leaf.State = proto.Int32(0)
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
	node.State = proto.Int32(0)
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
func insert(treenode TreeNode, record *RecordMetaData, tree *Btree) (rst, split bool, key []byte, left, right, refer *int32) {
	var dup_id *int32
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		rst  = clonenode.insert(record, tree)
		if len(clonenode.Keys) > int(*tree.info.Size) {
			key, left, right = clonenode.split(tree)
			if  *node.Id == *tree.info.Root {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, *left, *right, tree)
				tree.Lock()
				tree.info.Root = tnode.Id
				tree.Unlock()
			} else {
				split = true
			}
		}
		if rst && *node.Id == *tree.info.Root {
				tree.Lock()
				tree.info.Root = clonenode.Id
				tree.Unlock()
		} else {
			dup_id = clonenode.Id
			mark_dup(node, tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst  = cloneleaf.insert(record, tree)
		if len(cloneleaf.Records) > int(*tree.info.Size) {
			key, left, right = cloneleaf.split(tree)
			if *leaf.Id == *tree.info.Root {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, *left, *right, tree)
				tree.Lock()
				tree.info.Root = tnode.Id
				tree.Unlock()
			} else {
				split = true
			}
		}
		if rst && *leaf.Id == *tree.info.Root {
			tree.Lock()
			tree.info.Root = cloneleaf.Id
			tree.Unlock()
		} else {
			dup_id = cloneleaf.Id
			mark_dup(leaf, tree)
		}
	}
	refer = dup_id
	return
}
func (this *Node) insert(record *RecordMetaData, tree *Btree) (bool) {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key)
	rst, split, key, left, right, refer := insert(tree.nodes[this.Childrens[index]], record, tree)
	this.Childrens[index] = *refer
	if split && rst {
		this.insert_once(key, *left, *right, tree)
	}
	return rst
}
func (this *Leaf) insert(record *RecordMetaData, tree *Btree) (bool) {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key)
	if index > 0 {
		if bytes.Compare(this.Records[index-1].Key, record.Key) == 0 {
			return false
		}
	}
	this.Records = append(this.Records[:index], append([]*RecordMetaData{record}, this.Records[index:]...)...)
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
func delete(treenode TreeNode, key []byte, tree *Btree) (rst bool, refer *int32) {
	var dup_id *int32
	rst = false
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		if *node.Id == *tree.info.Root {
			tree.Lock()
			tree.cloneroot = clonenode.Id
			tree.Unlock()
		}
		if clonenode.delete(key, tree) {
			if *node.Id == *tree.info.Root {
				if len(clonenode.Keys) == 0 {
					tree.Lock()
					tmp := tree.nodes[*tree.cloneroot]
					tree.info.Root = get_id(clonenode.Childrens[0], tree)
					remove(tmp, tree)
					tree.Unlock()
				} else {
					tree.Lock()
					tree.info.Root = clonenode.Id
					tree.Unlock()
				}
			}
			rst = true
			dup_id = clonenode.Id
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst = cloneleaf.delete(key, tree)
		if *leaf.Id == *tree.info.Root {
			tree.Lock()
			tree.info.Root = cloneleaf.Id
			tree.Unlock()
		}
		dup_id = cloneleaf.Id
	}
	refer = dup_id
	return
}
func (this *Node) delete(key []byte, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(key)
	rst, refer :=  delete(tree.nodes[this.Childrens[index]], key, tree)
	if rst {
		this.Childrens[index] = *refer
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
					replace(key, removed_key, *tree.cloneroot, tree)
				}
			}
		}
		return true
	}
	return false
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
			if *tree.cloneroot != *this.Id {
				replace(key, this.Records[0].Key, *tree.cloneroot, tree)
			}
		}
		return true
	}
	return false
}
/*
 * Update
 */
func update(treenode TreeNode, record *RecordMetaData, tree *Btree) (bool, *int32) {
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		return clonenode.update(record, tree), clonenode.Id
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		return cloneleaf.update(record, tree), cloneleaf.Id
	}
	return false, nil
}
func (this *Node) update(record *RecordMetaData, tree *Btree) bool {
	this.Lock()
	defer this.Unlock()
	index := this.locate(record.Key)
	stat, clone := update(tree.nodes[this.Childrens[index]], record, tree)
	if stat {
		this.Childrens[index] = *clone
	}
	return stat
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
func (this *Leaf) split(tree *Btree) (key []byte, left *int32, right *int32) {
	newleaf := get_leaf(tree.newleaf(), tree)
	newleaf.Records = make([]*RecordMetaData, len(this.Records[*tree.info.Size/2:]))
	copy(newleaf.Records, this.Records[*tree.info.Size/2:])
	this.Records = this.Records[:*tree.info.Size/2]
	this.Next = newleaf.Id
	newleaf.Prev = this.Id
	left = this.Id
	right = newleaf.Id
	key = newleaf.Records[0].Key
	return
}
func (this *Node) split(tree *Btree) (key []byte, left *int32, right *int32) {
	newnode := get_node(tree.newnode(), tree)
	key = this.Keys[*tree.info.Size/2]
	newnode.Keys = make([][]byte, len(this.Keys[*tree.info.Size/2+1:]))
	copy(newnode.Keys, this.Keys[*tree.info.Size/2+1:])
	this.Keys = this.Keys[:*tree.info.Size/2]
	newnode.Childrens = make([]int32, len(this.Childrens[*tree.info.Size/2+1:]))
	copy(newnode.Childrens, this.Childrens[*tree.info.Size/2+1:])
	this.Childrens = this.Childrens[:*tree.info.Size/2+1]
	left = this.Id
	right = newnode.Id
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
	left.Next = right.Next
	if right.Next != nil {
		nextleaf := get_leaf(*right.Next, tree)
		nextleaf.Prev = left.Id
	}
	tree.Lock()
	remove(tree.nodes[*right.Id], tree)
	tree.Unlock()
}
func (this *Node) mergenode(left_id int32, right_id int32, index int, tree *Btree) {
	left_node := get_node(left_id, tree)
	right_node := get_node(right_id, tree)
	if len(left_node.Keys) + len(right_node.Keys) >  int(*tree.info.Size) {
		return
	}
	left_node.Keys = append(left_node.Keys, append([][]byte{this.Keys[index]}, right_node.Keys...)...)
	left_node.Childrens = append(left_node.Childrens, right_node.Childrens...)
	this.Keys = append(this.Keys[:index],this.Keys[index+1:]...)
	this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
	tree.Lock()
	remove(tree.nodes[*right_node.Id], tree)
	tree.Unlock()
	if len(left_node.Keys) > int(*tree.info.Size) {
		key, left, right := left_node.split(tree)
		this.insert_once(key, *left, *right, tree)
	}
}

func remove(treenode TreeNode, tree *Btree) {
	if node, ok := treenode.(*Node); ok {
		tree.info.FreeNodeList = append(tree.info.FreeNodeList, *node.Id)
		node.Keys = node.Keys[:0]
		node.Childrens = node.Childrens[:0]
		node.State = proto.Int32(-1)
		*tree.info.NodeCount --
	}
	if leaf, ok := treenode.(*Leaf); ok {
		tree.info.FreeLeafList = append(tree.info.FreeLeafList, *leaf.Id)
		leaf.State = proto.Int32(-1)
		leaf.Records = leaf.Records[:0]
		*tree.info.LeafCount --
	}
}
func mark_dup(treenode TreeNode, tree *Btree) {
	if node, ok := treenode.(*Node); ok {
		node.State = proto.Int32(1)
		*tree.info.NodeCount --
	}
	if leaf, ok := treenode.(*Leaf); ok {
		leaf.State = proto.Int32(1)
		*tree.info.LeafCount --
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
func get_id(id int32, tree *Btree) *int32 {
	if node, ok := tree.nodes[id].(*Node); ok {
		return node.Id
	}
	if leaf, ok := tree.nodes[id].(*Leaf); ok {
		return leaf.Id
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
func (this *Btree) clonenode(node *Node) (*Node) {
	newnode := get_node(this.newnode(), this)
	newnode.Keys = make([][]byte, len(node.Keys))
	copy(newnode.Keys, node.Keys)
	newnode.Childrens = make([]int32, len(node.Childrens))
	copy(newnode.Childrens, node.Childrens)
	return newnode
}
func (this *Btree) cloneleaf(leaf *Leaf) (*Leaf) {
	if len(leaf.Records) == 0 {
		return leaf
	}
	newleaf := get_leaf(this.newleaf(), this)
	newleaf.Records = make([]*RecordMetaData, len(leaf.Records))
	copy(newleaf.Records, leaf.Records)
	newleaf.Prev = leaf.Prev
	newleaf.Next = leaf.Next
	return newleaf
}
func (this *Btree)free_node_count() int32 {
	return *this.info.NodeMax - *this.info.NodeCount
}
func (this *Btree)free_leaf_count() int32 {
	return *this.info.LeafMax - *this.info.LeafCount
}
