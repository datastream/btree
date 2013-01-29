package btree_test

import (
	"../btree"
	"strconv"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	tree := btree.NewBtreeSize(2, 2)
	size := 100
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go func() {
			if stat := tree.Insert(rd); !stat {
				t.Fatal("Insert Failed", i)
			}
		}()
	}
}
func TestSearch(t *testing.T) {
	tree := btree.NewBtreeSize(2, 3)
	size := 100000
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd)
	}
	time.Sleep(time.Second * 10)
	for i := 0; i < size; i++ {
		if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i) {
			t.Fatal("Find Failed", i)
		}
	}
}
func TestUpdate(t *testing.T) {
	tree := btree.NewBtreeSize(3, 2)
	size := 100000
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		tree.Insert(rd)
	}
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i + 1)),
		}
		if stat := tree.Update(rd); !stat {
			t.Fatal("Update Failed", i)
		}
	}
	for i := 0; i < size; i++ {
		if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i+1) {
			t.Fatal("Find Failed", i)
		}
	}
}
func TestDelete(t *testing.T) {
	tree := btree.NewBtreeSize(3, 3)
	size := 100
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		tree.Insert(rd)
	}
	for i := 0; i < size; i++ {
		if stat := tree.Delete([]byte(strconv.Itoa(i))); !stat {
			t.Fatal("delete Failed", i)
		}
		if tree.Search([]byte(strconv.Itoa(i))) != nil {
			t.Fatal("Find Failed", i)
		}
	}
}

func TestDump(t *testing.T) {
	tree := btree.NewBtreeSize(3, 3)
	size := 100
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		tree.Insert(rd)
	}
	tree.Dump("treedump")
}
func TestRestore(t *testing.T) {
	size := 100
	if tree, err := btree.Restore("treedump_100"); err == nil {
		for i := 0; i < size; i++ {
			if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i) {
				t.Fatal("Find Failed", i)
			}
		}
	}
}
func BenchmarkBtreeInsert(t *testing.B) {
	size := 100000
	tree := btree.NewBtree()
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		if stat := tree.Insert(rd); !stat {
			t.Fatal("Insert Failed", i)
		}
	}
	tree.Dump("treedump")
}
func BenchmarkBtreeSearch(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_100000"); err == nil {
		for i := 0; i < size; i++ {
			if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i) {
				t.Fatal("Find Failed", i)
			}
		}
	}
}
func BenchmarkBtreeUpdate(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_100000"); err == nil {
		for i := 0; i < size; i++ {
			rd := &btree.Record{
				Key:   []byte(strconv.Itoa(i)),
				Value: []byte(strconv.Itoa(i + 1)),
			}
			if stat := tree.Update(rd); !stat {
				t.Fatal("Update Failed", i)
			}
		}
	}
}
func BenchmarkBtreeDelete(t *testing.B) {
	size := 100000
	if tree, err := btree.Restore("treedump_100000"); err == nil {
		for i := 0; i < size; i++ {
			if stat := tree.Delete([]byte(strconv.Itoa(i))); !stat {
				t.Fatal("delete Failed", i)
			}
		}
	}
}
