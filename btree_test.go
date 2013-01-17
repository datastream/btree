package btree_test

import (
	"../btree"
	"strconv"
	"testing"
)

func testBtreeInsert(t *testing.T, tree *btree.Btree, size int) {
	rst := make(chan bool)
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd, rst)
		stat := <-rst
		if !stat {
			t.Fatal("Insert Failed", i)
		}
	}
}
func testBtreeSearch(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if rst == nil {
			t.Fatal("Find Failed", i)
		}
	}
}
func testBtreeUpdate(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i + 1)),
		}
		u_rst := make(chan bool)
		go tree.Update(rd, u_rst)
		stat := <-u_rst
		if !stat {
			t.Fatal("Update Failed", i)
		}
	}
}
func testBtreeDeleteCheck(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if rst == nil {
			t.Fatal("Find Failed", i)
		}
		d_rst := make(chan bool)
		go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
		stat := <-d_rst
		q_rst = make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst = <-q_rst
		if rst != nil {
			t.Fatal("Find deleted key", i)
		}
		if !stat {
			t.Fatal("delete Failed", i)
		}
	}
}
func testBtreeDelete(t *testing.T, tree *btree.Btree, size int) {
	for i := 0; i < size; i++ {
		d_rst := make(chan bool)
		go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
		stat := <-d_rst
		if !stat {
			t.Fatal("delete Failed", i)
		}
	}
}

func TestBtree(t *testing.T) {
	tree := btree.NewBtree()
	size := 100000
	testBtreeInsert(t, tree, size)
	testBtreeSearch(t, tree, size)
	testBtreeUpdate(t, tree, size)
	testBtreeSearch(t, tree, size)
	testBtreeDeleteCheck(t, tree, size)
	testBtreeInsert(t, tree, size)
	tree.Dump("treedump")
	if ntree, err := btree.Restore("treedump_0"); err == nil {
		testBtreeSearch(t, ntree, size)
		testBtreeUpdate(t, ntree, size)
		testBtreeDelete(t, ntree, size)
	}
}

func BenchmarkBtreeInsert(t *testing.B) {
	size := 100000
	tree := btree.NewBtree()
	rst := make(chan bool)
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd, rst)
		stat := <-rst
		if !stat {
			t.Fatal("Insert Failed", i)
		}
	}
}
func BenchmarkBtreeSearch(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_0"); err == nil {
		for i := 0; i < size; i++ {
			q_rst := make(chan []byte)
			go tree.Search([]byte(strconv.Itoa(i)), q_rst)
			rst := <-q_rst
			if rst == nil {
				t.Fatal("Find Failed", i)
			}
		}
	}
}
func BenchmarkBtreeUpdate(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_0"); err == nil {
		for i := 0; i < size; i++ {
			rd := &btree.Record{
				Key:   []byte(strconv.Itoa(i)),
				Value: []byte(strconv.Itoa(i + 1)),
			}
			u_rst := make(chan bool)
			go tree.Update(rd, u_rst)
			stat := <-u_rst
			if !stat {
				t.Fatal("Update Failed", i)
			}
		}
	}
}
func BenchmarkBtreeDeleteCheck(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_0"); err == nil {
		for i := 0; i < size; i++ {
			q_rst := make(chan []byte)
			go tree.Search([]byte(strconv.Itoa(i)), q_rst)
			rst := <-q_rst
			if rst == nil {
				t.Fatal("Find Failed", i)
			}
			d_rst := make(chan bool)
			go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
			stat := <-d_rst
			q_rst = make(chan []byte)
			go tree.Search([]byte(strconv.Itoa(i)), q_rst)
			rst = <-q_rst
			if rst != nil {
				t.Fatal("Find deleted key", i)
			}
			if !stat {
				t.Fatal("delete Failed", i)
			}
		}
	}
}
func BenchmarkBtreeDelete(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_0"); err == nil {
		for i := 0; i < size; i++ {
			d_rst := make(chan bool)
			go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
			stat := <-d_rst
			if !stat {
				t.Fatal("delete Failed", i)
			}
		}
	}
}

func BenchmarkBtree(t *testing.B) {
	BenchmarkBtreeInsert(t)
	BenchmarkBtreeSearch(t)
	BenchmarkBtreeUpdate(t)
	BenchmarkBtreeDelete(t)
	BenchmarkBtreeDeleteCheck(t)
}
