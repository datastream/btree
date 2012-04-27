package btree

import (
	"sync"
	"bytes"
	"log"
	"bufio"
	"strconv"
	"os"
	"code.google.com/p/goprotobuf/proto"
)

const SIZE = 1<<10
const LEAFSIZE = 1 << 5
const NODESIZE = 1 << 6

type Btree struct {
	info *BtreeMetaData
	nodes []TreeNode
	sync.Mutex
	stat int
	cloneroot *int32
	dupnodelist []int32
	current_version int32
}
type  Leaf struct {
	LeafMetaData
}
type Node struct {
	NodeMetaData
}
type TreeNode interface {
	insert(record *RecordMetaData, tree *Btree) bool
	delete(key []byte, tree *Btree) bool
	update(recode *RecordMetaData, tree *Btree) bool
	search(key []byte, tree *Btree) []byte
}

func NewBtree() *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.stat = 0
	tree.current_version = 0
	tree.info = &BtreeMetaData{
	Size: proto.Int32(SIZE),
	LeafMax:  proto.Int32(LEAFSIZE),
	NodeMax: proto.Int32(NODESIZE),
	LeafCount: proto.Int32(0),
	NodeCount:  proto.Int32(0),
	IndexCursor: proto.Int32(0),
	}
	tree.info.Version = proto.Int32(0)
	tree.info.Root =  proto.Int32(tree.newleaf())
	return tree
}

func NewBtreeSize(leafsize uint32, nodesize uint32) *Btree {
	tree := new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.stat = 0
	tree.current_version = 0
	tree.info = &BtreeMetaData{
	Size: proto.Int32(SIZE),
	LeafMax:  proto.Int32(LEAFSIZE),
	NodeMax: proto.Int32(NODESIZE),
	LeafCount: proto.Int32(0),
	NodeCount:  proto.Int32(0),
	IndexCursor: proto.Int32(0),
	}
	tree.info.Version = proto.Int32(0)
	tree.info.Root =  proto.Int32(tree.newleaf())
	return tree
}

func (this *Btree) Insert(record *RecordMetaData, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.info.Version ++
		this.stat ++
	}
	stat, _, _, _, _, _ := insert(this.nodes[*this.info.Root], record, this)
	rst <- stat
}

func (this *Btree) Delete(key []byte, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.info.Version ++
		this.stat ++
	}
	stat, _ :=  delete(this.nodes[*this.info.Root], key, this)
	rst <- stat
}

func (this *Btree) Search(key []byte, rst chan []byte) {
	rst <- search(this.nodes[*this.info.Root], key, this)
}

func (this *Btree) Update(record *RecordMetaData, rst chan bool) {
	this.Lock()
	defer this.Unlock()
	if this.stat == 1 {
		*this.info.Version ++
		this.stat ++
	}
	stat, _ := update(this.nodes[*this.info.Root], record, this)
	rst <- stat
}
/*
 * DUMP/RESTORE
 */
func (this *Btree) Dump(filename string) error {
	this.Lock()
	this.stat = 1
	snapversion := *this.info.Version
	size := *this.info.IndexCursor
	this.Unlock()
	file, err := os.Open(filename + strconv.Itoa(int(snapversion)))
	if err != nil {
		log.Fatal("file open failed ", filename , "at version" ,snapversion)
		return err
	}
	fb := bufio.NewWriterSize(file, 1048576)
	for i := 0; i < int(size); i++ {
		if leaf, ok := this.nodes[i].(*Leaf); ok {
			if *leaf.Version < snapversion {
				data, err := proto.Marshal(leaf)
				if err != nil {
					log.Fatal("encode error ",i)
				} else {
					_, err := fb.Write(data)
					if err != nil {
						log.Fatal("write file error", err,"at version", snapversion)
						return err
					}
				}
			}
		}
		if node, ok := this.nodes[i].(*Node); ok {
			if *node.Version < snapversion {
				data, err := proto.Marshal(node)
				if err != nil {
					log.Fatal("encode error ",i)
				} else {
					_, err := fb.Write(data)
					if err != nil {
						log.Fatal("write file error", err, "at version", snapversion)
						return err
					}
				}
			}
		}
	}
	fb.Flush()
	file.Close()
	go this.gc()
	this.Lock()
	this.stat = 0
	this.Unlock()
	return nil
}
/*
 * alloc leaf/node
 */
func (this *Btree) newleaf() int32 {
	var id int32
	*this.info.LeafCount ++
	leaf := new(Leaf)
	leaf.State = proto.Int32(0)
	if len(this.info.FreeList) > 0 {
		id = this.info.FreeList[len(this.info.FreeList)-1]
		this.info.FreeList = this.info.FreeList[:len(this.info.FreeList)-1]
	} else {
		if *this.info.IndexCursor >= *this.info.Size {
			this.nodes = append(this.nodes, make([]TreeNode, SIZE)...)
			*this.info.Size += int32(SIZE)
		}
		id = *this.info.IndexCursor
		*this.info.IndexCursor ++
	}
	leaf.Id = proto.Int32(id)
	leaf.Version = proto.Int32(*this.info.Version)
	this.nodes[*leaf.Id] = leaf
	return id
}
func (this *Btree) newnode() int32 {
	var id int32
	*this.info.NodeCount ++
	node := new(Node)
	node.State = proto.Int32(0)
	if len(this.info.FreeList) > 0 {
		id = this.info.FreeList[len(this.info.FreeList)-1]
		this.info.FreeList = this.info.FreeList[:len(this.info.FreeList)-1]
	} else {
		if *this.info.IndexCursor >= *this.info.Size {
			this.nodes = append(this.nodes, make([]TreeNode, SIZE)...)
			*this.info.Size += int32(SIZE)
		}
		id = *this.info.IndexCursor
		*this.info.IndexCursor ++
	}
	node.Id = proto.Int32(id)
	node.Version = proto.Int32(*this.info.Version)
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
		if len(clonenode.Keys) > int(*tree.info.NodeMax) {
			key, left, right = clonenode.split(tree)
			if  *node.Id == *tree.info.Root {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, *left, *right, tree)
				tree.info.Root = tnode.Id
			} else {
				split = true
			}
		}
		if rst {
			if *node.Id == *tree.info.Root {
				tree.info.Root = clonenode.Id
			}
			dup_id = clonenode.Id
			mark_dup(*node.Id, tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst  = cloneleaf.insert(record, tree)
		if len(cloneleaf.Records) > int(*tree.info.LeafMax) {
			key, left, right = cloneleaf.split(tree)
			if *leaf.Id == *tree.info.Root {
				tnode := get_node(tree.newnode(), tree)
				tnode.insert_once(key, *left, *right, tree)
				tree.info.Root = tnode.Id
			} else {
				split = true
			}
		}
		if rst {
			if *leaf.Id == *tree.info.Root {
				tree.info.Root = cloneleaf.Id
			}
			dup_id = cloneleaf.Id
			mark_dup(*leaf.Id, tree)
		}
	}
	refer = dup_id
	return
}
func (this *Node) insert(record *RecordMetaData, tree *Btree) (bool) {
	index := this.locate(record.Key)
	rst, split, key, left, right, refer := insert(tree.nodes[this.Childrens[index]], record, tree)
	if rst {
		this.Childrens[index] = *refer
		if split {
			this.insert_once(key, *left, *right, tree)
		}
	} else {
		remove(*this.Id, tree)
	}
	return rst
}
func (this *Leaf) insert(record *RecordMetaData, tree *Btree) (bool) {
	index := this.locate(record.Key)
	if index > 0 {
		if bytes.Compare(this.Records[index-1].Key, record.Key) == 0 {
			remove(*this.Id, tree)
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
	index := this.locate(key)
	return search(tree.nodes[this.Childrens[index]], key, tree)
}
func (this *Leaf) search(key []byte, tree *Btree) []byte {
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
			tree.cloneroot = clonenode.Id
		}
		if rst = clonenode.delete(key, tree); rst {
			if *node.Id == *tree.info.Root {
				if len(clonenode.Keys) == 0 {
					tree.info.Root = get_id(clonenode.Childrens[0], tree)
					remove(*tree.cloneroot, tree)
				} else {
					tree.info.Root = clonenode.Id
				}
			}
			dup_id = clonenode.Id
			mark_dup(*node.Id, tree)
		}
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		if rst = cloneleaf.delete(key, tree); rst {
			if *leaf.Id == *tree.info.Root {
				tree.info.Root = cloneleaf.Id
			}
			dup_id = cloneleaf.Id
			mark_dup(*leaf.Id, tree)
		}
	}
	refer = dup_id
	return
}
func (this *Node) delete(key []byte, tree *Btree) bool {
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
	remove(*this.Id, tree)
	return false
}
func (this *Leaf) delete(key []byte, tree *Btree) bool {
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
	remove(*this.Id, tree)
	return false
}
/*
 * Update
 */
func update(treenode TreeNode, record *RecordMetaData, tree *Btree) (rst bool, refer *int32) {
	if node, ok := treenode.(*Node); ok {
		clonenode := tree.clonenode(node)
		rst = clonenode.update(record, tree)
		if rst {
			refer = clonenode.Id
			if *tree.info.Root == *node.Id {
				tree.info.Root = clonenode.Id
			}
			mark_dup(*node.Id, tree)
		}
		return
	}
	if leaf, ok := treenode.(*Leaf); ok {
		cloneleaf := tree.cloneleaf(leaf)
		rst = cloneleaf.update(record, tree)
		if rst {
			refer = cloneleaf.Id
			if *tree.info.Root == *leaf.Id {
				tree.info.Root = cloneleaf.Id
			}
			mark_dup(*leaf.Id, tree)
		}
		return
	}
	return
}
func (this *Node) update(record *RecordMetaData, tree *Btree) bool {
	index := this.locate(record.Key)
	stat, clone := update(tree.nodes[this.Childrens[index]], record, tree)
	if stat {
		this.Childrens[index] = *clone
	} else {
		remove(*this.Id, tree)
	}
	return stat
}

func (this *Leaf) update(record *RecordMetaData, tree *Btree) bool {
	index := this.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(this.Records[index].Key, record.Key) == 0 {
			this.Records[index].Value = record.Value
			return true
		}
	}
	remove(*this.Id, tree)
	return false
}
/*
 * Split
 */
func (this *Leaf) split(tree *Btree) (key []byte, left *int32, right *int32) {
	newleaf := get_leaf(tree.newleaf(), tree)
	newleaf.Records = make([]*RecordMetaData, len(this.Records[*tree.info.LeafMax/2:]))
	copy(newleaf.Records, this.Records[*tree.info.LeafMax/2:])
	this.Records = this.Records[:*tree.info.LeafMax/2]
	left = this.Id
	right = newleaf.Id
	key = newleaf.Records[0].Key
	return
}
func (this *Node) split(tree *Btree) (key []byte, left *int32, right *int32) {
	newnode := get_node(tree.newnode(), tree)
	key = this.Keys[*tree.info.NodeMax/2]
	newnode.Keys = make([][]byte, len(this.Keys[*tree.info.NodeMax/2+1:]))
	copy(newnode.Keys, this.Keys[*tree.info.NodeMax/2+1:])
	this.Keys = this.Keys[:*tree.info.NodeMax/2]
	newnode.Childrens = make([]int32, len(this.Childrens[*tree.info.NodeMax/2+1:]))
	copy(newnode.Childrens, this.Childrens[*tree.info.NodeMax/2+1:])
	this.Childrens = this.Childrens[:*tree.info.NodeMax/2+1]
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
	if (len(left.Records) + len(right.Records)) > int(*tree.info.LeafMax) {
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
	mark_dup(*right.Id, tree)
}
func (this *Node) mergenode(left_id int32, right_id int32, index int, tree *Btree) {
	left_node := get_node(left_id, tree)
	right_node := get_node(right_id, tree)
	if len(left_node.Keys) + len(right_node.Keys) >  int(*tree.info.NodeMax) {
		return
	}
	left_node.Keys = append(left_node.Keys, append([][]byte{this.Keys[index]}, right_node.Keys...)...)
	left_node.Childrens = append(left_node.Childrens, right_node.Childrens...)
	this.Keys = append(this.Keys[:index],this.Keys[index+1:]...)
	this.Childrens = append(this.Childrens[:index+1], this.Childrens[index+2:]...)
	mark_dup(*right_node.Id, tree)
	if len(left_node.Keys) > int(*tree.info.NodeMax) {
		key, left, right := left_node.split(tree)
		this.insert_once(key, *left, *right, tree)
	}
}

func remove(index int32, tree *Btree) {
	if node, ok := tree.nodes[index].(*Node); ok {
		tree.info.FreeList = append(tree.info.FreeList, *node.Id)
		*tree.info.NodeCount --
	}
	if leaf, ok := tree.nodes[index].(*Leaf); ok {
		tree.info.FreeList = append(tree.info.FreeList, *leaf.Id)
		*tree.info.LeafCount --
	}
	tree.nodes[index] = nil
}
func mark_dup(index int32, tree *Btree) {
	if tree.stat == 1 {
		if node, ok := tree.nodes[index].(*Node); ok {
			node.State = proto.Int32(1)
			*tree.info.NodeCount --
		}
		if leaf, ok := tree.nodes[index].(*Leaf); ok {
			leaf.State = proto.Int32(1)
			*tree.info.LeafCount --
		}
		tree.dupnodelist = append(tree.dupnodelist, index)
	} else {
		remove(index, tree)
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
	newleaf := get_leaf(this.newleaf(), this)
	newleaf.Records = make([]*RecordMetaData, len(leaf.Records))
	copy(newleaf.Records, leaf.Records)
	return newleaf
}
func (this *Btree)tree_size() int32 {
	return *this.info.Size
}
func (this *Btree)gc() {
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
