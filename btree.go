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

func (T *Btree) Insert(record *RecordMetaData, rst chan bool) {
	T.Lock()
	if T.stat > 0 {
		T.cond.Wait()
	}
	if T.free_node_count() < 50 || T.free_leaf_count() < 50 {
		rst <- false
		return
	}
	T.Unlock()
	rst <- insert(T.nodes[*T.info.Root], record, T)
}

func (T *Btree) Delete(key []byte, rst chan bool) {
	T.Lock()
	if T.stat > 0 {
		T.cond.Wait()
	}
	T.Unlock()
	rst <- delete(T.nodes[*T.info.Root], key, T)
}

func (T *Btree) Search(key []byte, rst chan []byte) {
	T.Lock()
	if T.stat > 0 {
		T.cond.Wait()
	}
	T.Unlock()
	rst <- search(T.nodes[*T.info.Root], key, T)
}

func (T *Btree) Update(record *RecordMetaData, rst chan bool) {
	T.Lock()
	if T.stat > 0 {
		T.cond.Wait()
	}
	T.Unlock()
	rst <- update(T.nodes[*T.info.Root], record, T)
}
/*
 * alloc leaf/node
 */
func (T *Btree) newleaf() int32 {
	T.Lock()
	defer T.Unlock()
	var id int32
	*T.info.LastLeaf ++
	*T.info.LeafCount ++
	leaf := new(Leaf)
	leaf.Removed = proto.Bool(false)
	if len(T.info.FreeLeafList) > 0 {
		id = T.info.FreeLeafList[len(T.info.FreeLeafList)-1]
		T.info.FreeLeafList = T.info.FreeLeafList[:len(T.info.FreeLeafList)-1]
	} else {
		id = *T.info.LastLeaf
	}
	leaf.Id = proto.Int32(id)
	T.nodes[*leaf.Id] = leaf
	return id
}
func (T *Btree) newnode() int32 {
	T.Lock()
	defer T.Unlock()
	var id int32
	*T.info.LastNode ++
	*T.info.NodeCount ++
	node := new(Node)
	node.Removed = proto.Bool(false)
	if len(T.info.FreeNodeList) > 0 {
		id = T.info.FreeNodeList[len(T.info.FreeNodeList)-1]
		T.info.FreeNodeList = T.info.FreeNodeList[:len(T.info.FreeNodeList)-1]
	} else {
		id = *T.info.LastNode
	}
	node.Id = proto.Int32(id)
	T.nodes[*node.Id] = node
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
func (N *Node) insert(record *RecordMetaData, tree *Btree) bool {
	N.Lock()
	defer N.Unlock()
	index := N.locate(record.Key)
	return insert(tree.nodes[N.Childrens[index]], record, tree)
}
func (L *Leaf) insert(record *RecordMetaData, tree *Btree) bool {
	L.Lock()
	defer L.Unlock()
	index := L.locate(record.Key)
	if index > 0 {
		if bytes.Compare(L.Records[index-1].Key, record.Key) == 0 {
			return false
		}
	}
	L.Records = append(L.Records[:index], append([]*RecordMetaData{record}, L.Records[index:]...)...)
	if uint32(len(L.Records)) > *tree.info.Size {
		L.split(tree)
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
func (N *Node) search(key []byte, tree *Btree) []byte {
	N.RLock()
	defer N.RUnlock()
	index := N.locate(key)
	return search(tree.nodes[N.Childrens[index]], key, tree)
}
func (L *Leaf) search(key []byte, tree *Btree) []byte {
	L.RLock()
	defer L.RUnlock()
	index := L.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(L.Records[index].Key, key) == 0 {
			return L.Records[index].Value
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
func (N *Node) delete(key []byte, tree *Btree) bool {
	N.Lock()
	defer N.Unlock()
	index := N.locate(key)
	return delete(tree.nodes[N.Childrens[index]], key, tree)
}
func (L *Leaf) delete(key []byte, tree *Btree) bool {
	L.Lock()
	defer L.Unlock()
	var deleted bool
	index := L.locate(key) -1
	if index >= 0 {
		if bytes.Compare(L.Records[index].Key, key) == 0 {
			deleted = true
		}
	}
	if deleted {
		L.Records = append(L.Records[:index],L.Records[index+1:]...)
		if index == 0 && len(L.Records) > 0 {
			if tree.info.Root != L.Id {
				replace(key, L.Records[0].Key, *L.Father, tree)
			}
		}
		if L.Id != tree.info.Root {
			node := tree.nodes[*L.Father]
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
func (N *Node) update(record *RecordMetaData, tree *Btree) bool {
	N.Lock()
	defer N.Unlock()
	index := N.locate(record.Key)
	return tree.nodes[N.Childrens[index]].update(record, tree)
}

func (L *Leaf) update(record *RecordMetaData, tree *Btree) bool {
	L.Lock()
	defer L.Unlock()
	index := L.locate(record.Key) - 1
	if index >= 0 {
		if bytes.Compare(L.Records[index].Key, record.Key) == 0 {
			L.Records[index].Value = record.Value
			return true
		}
	}
	return false
}
/*
 * Split
 */
func (L *Leaf) split(tree *Btree) {
	newleaf := get_leaf(tree.newleaf(), tree)
	newleaf.Records = make([]*RecordMetaData, len(L.Records[*tree.info.Size/2:]))
	copy(newleaf.Records, L.Records[*tree.info.Size/2:])
	L.Records = L.Records[:*tree.info.Size/2]
	L.Next = newleaf.Id
	newleaf.Prev = L.Id
	if *tree.info.NodeCount != 0 {
		tnode := get_node(*L.Father, tree)
		newleaf.Father = L.Father
		tnode.insert_once(newleaf.Records[0].Key, *L.Id, *newleaf.Id, tree)
	} else {
		tnode := get_node(tree.newnode(), tree)
		tnode.insert_once(newleaf.Records[0].Key, *L.Id, *newleaf.Id, tree)
		L.Father = tnode.Id
		newleaf.Father = L.Father
		tree.Lock()
		tree.info.Root = tnode.Id
		tree.Unlock()
	}
}
func (N *Node) split(tree *Btree) {
	newnode := get_node(tree.newnode(), tree)
	key := N.Keys[*tree.info.Size/2]
	newnode.Keys = make([][]byte, len(N.Keys[*tree.info.Size/2+1:]))
	copy(newnode.Keys, N.Keys[*tree.info.Size/2+1:])
	N.Keys = N.Keys[:*tree.info.Size/2]
	newnode.Childrens = make([]int32, len(N.Childrens[*tree.info.Size/2+1:]))
	copy(newnode.Childrens, N.Childrens[*tree.info.Size/2+1:])
	N.Childrens = N.Childrens[:*tree.info.Size/2+1]
	for l := 0; l < len(newnode.Childrens); l++ {
		set_father(tree.nodes[newnode.Childrens[l]], newnode.Id)
	}
	if N.Id == tree.info.Root {
		tnode := get_node(tree.newnode(), tree)
		N.Father = tnode.Id
		newnode.Father = N.Father
		tnode.insert_once(key, *N.Id, *newnode.Id, tree)
		tree.Lock()
		tree.info.Root = tnode.Id
		tree.Unlock()
	} else {
		newnode.Father = N.Father
		tnode := get_node(*N.Father, tree)
		tnode.insert_once(key, *N.Id, *newnode.Id, tree)
	}

}
/*
 * insert key into tree node
 */
func (N *Node) insert_once(key []byte, left_id int32, right_id int32, tree *Btree) {
	index := N.locate(key)
	if len(N.Keys) == 0 {
		N.Childrens = append([]int32{left_id}, right_id)
	} else {
		N.Childrens = append(N.Childrens[:index+1], append([]int32{right_id}, N.Childrens[index+1:]...)...)
	}
	N.Keys = append(N.Keys[:index], append([][]byte{key}, N.Keys[index:]...)...)
	if len(N.Keys) > int(*tree.info.Size) {
		N.split(tree)
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

func (N *Node) mergeleaf(left_id int32, right_id int32, index int, tree *Btree) {
	left := get_leaf(left_id, tree)
	right := get_leaf(right_id, tree)
	if (len(left.Records) + len(right.Records)) > int(*tree.info.Size) {
		return
	}
	if index == len(N.Keys) {
		N.Childrens = N.Childrens[:index]
		N.Keys = N.Keys[:index-1]
	} else {
		N.Childrens = append(N.Childrens[:index+1], N.Childrens[index+2:]...)
		N.Keys = append(N.Keys[:index],N.Keys[index+1:]...)
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
	if N.Id != tree.info.Root {
		node := get_node(*N.Father, tree)
		merge(left.Records[0].Key, node, tree)
	} else {
		tree.Lock()
		if len(N.Keys) == 0 {
			remove(tree.nodes[*tree.info.Root], tree)
			tree.info.Root = left.Id
		}
		tree.Unlock()
	}
}
func (N *Node) mergenode(left_id int32, right_id int32, index int, tree *Btree) {
	left := get_node(left_id, tree)
	right := get_node(right_id, tree)
	if len(left.Keys) + len(right.Keys) >  int(*tree.info.Size) {
		return
	}
	key := N.Keys[0]
	for l := 0; l < len(right.Childrens); l++ {
		set_father(tree.nodes[right.Childrens[l]], left.Id)
	}
	left.Keys = append(left.Keys, append([][]byte{N.Keys[index]}, right.Keys...)...)
	left.Childrens = append(left.Childrens, right.Childrens...)
	right.Keys = right.Keys[:0]
	right.Childrens = right.Childrens[:0]
	N.Keys = append(N.Keys[:index],N.Keys[index+1:]...)
	N.Childrens = append(N.Childrens[:index+1], N.Childrens[index+2:]...)
	tree.Lock()
	remove(tree.nodes[*right.Id], tree)
	tree.Unlock()
	if len(left.Keys) > int(*tree.info.Size) {
		left.split(tree)
	} else {
		if N.Id != tree.info.Root {
			node := get_node(*N.Father, tree)
			merge(key, node, tree)
		} else {
			tree.Lock()
			if len(N.Keys) == 0 {
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
func (N *Node) locate(key []byte) (int) {
	i := 0
	size := len(N.Keys)
	for {
		mid := (i+size)/2
		if i == size {
			break
		}
		if bytes.Compare(N.Keys[mid], key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}
func (L *Leaf) locate(key []byte) (int) {
	i := 0
	size := len(L.Records)
	for {
		mid := (i+size)/2
		if i == size {
			break
		}
		if bytes.Compare(L.Records[mid].Key, key) <= 0 {
			i = mid + 1
		} else {
			size = mid
		}
	}
	return i
}
func (T *Btree)free_node_count() int32 {
	return *T.info.NodeMax - *T.info.NodeCount
}
func (T *Btree)free_leaf_count() int32 {
	return *T.info.LeafMax - *T.info.LeafCount
}
