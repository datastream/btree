package btree

import (
	"bytes"
)

// Search return value
func (t *Btree) search(key []byte) []byte {
	return t.nodes[t.GetRoot()].searchRecord(key, t)
}

// node search record
func (n *Node) searchRecord(key []byte, tree *Btree) []byte {
	index := n.locate(key)
	return tree.nodes[n.Childrens[index]].searchRecord(key, tree)
}

// leaf search
func (l *Leaf) searchRecord(key []byte, tree *Btree) []byte {
	index := l.locate(key) - 1
	if index >= 0 {
		if bytes.Compare(l.Keys[index], key) == 0 {
			return l.Values[index]
		}
	}
	return nil
}
