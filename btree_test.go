package btree_test

import (
	"strconv"
	"../btree"
	"testing"
	)

func testBtreeInsert(t *testing.T, tree *btree.Btree, size int) {
	rst := make(chan bool)
	for i := 0; i < size;i ++ {
		rd := &btree.RecordMetaData {
		Key:[]byte(strconv.Itoa(i)),
		Value:[]byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd, rst)
		stat := <- rst
		if !stat {
			t.Fatal("Insert Failed",i)
		}
	}
}
func testBtreeSearch(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if rst == nil {
			t.Fatal("Find Failed",i)
		}
	}
}
func testBtreeUpdate(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		rd := &btree.RecordMetaData {
		Key:[]byte(strconv.Itoa(i)),
		Value:[]byte(strconv.Itoa(i+1)),
		}
		u_rst := make(chan bool)
		go tree.Update(rd, u_rst)
		stat := <- u_rst
		if !stat {
			t.Fatal("Update Failed",i)
		}
	}
}
func testBtreeDeleteCheck(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if rst == nil {
			t.Fatal("Find Failed",i)
		}
		d_rst := make(chan bool)
		go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
		stat := <- d_rst
		q_rst = make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst = <-q_rst
		if rst != nil {
			t.Fatal("Find deleted key",i)
		}
		if !stat {
			t.Fatal("delete Failed",i)
		}
	}
}
func testBtreeDelete(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		d_rst := make(chan bool)
		go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
		stat := <- d_rst
		if !stat {
			t.Fatal("delete Failed",i)
		}
	}
}

func TestBtree(t *testing.T) {
	tree := btree.NewBtreeSize(5)
	testBtreeInsert(t, tree, 200)
	testBtreeSearch(t, tree, 200)
	testBtreeUpdate(t, tree, 200)
	testBtreeSearch(t, tree, 200)
	testBtreeDeleteCheck(t, tree, 200)
	testBtreeInsert(t, tree, 200)
	testBtreeDelete(t, tree, 200)
}
