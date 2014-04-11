package btree

import (
	"bufio"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"os"
)

// Marshal btree to disk
func (t *Btree) Marshal(filename string) error {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()
	fb := bufio.NewWriter(fd)
	data, err := proto.Marshal(&t.BtreeMetadata)
	if err != nil {
		return err
	}
	_, err = fb.Write(data)
	if err != nil {
		return err
	}
	return fb.Flush()
}

// Unmarshal btree from disk
func Unmarshal(filename string) (*Btree, error) {
	tree := &Btree{
		dupnodelist: make(map[int64]int),
		opChan:      make(chan *treeOperation),
	}
	fd, err := os.Open(filename)
	if err != nil {
		return tree, err
	}
	defer fd.Close()
	dataRecord, err := ioutil.ReadAll(fd)
	if err != nil {
		return tree, err
	}
	tree.BtreeMetadata = BtreeMetadata{}
	proto.Unmarshal(dataRecord, &tree.BtreeMetadata)
	go tree.run()
	return tree, err
}
