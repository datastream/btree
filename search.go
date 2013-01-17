package btree

import (
	"bytes"
)

// search record
func (this *Node) search_record(key []byte, tree *Btree) []byte {
	index := this.locate(key)
	return tree.nodes[this.Childrens[index]].search_record(key, tree)
}

func (this *Leaf) search_record(key []byte, tree *Btree) []byte {
	index := this.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(this.Keys[index], key) == 0 {
			return this.Values[index]
		}
	}
	return nil
}
