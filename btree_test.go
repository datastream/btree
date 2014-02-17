package btree_test

import (
	"../btree"
	"strconv"
	"testing"
)

func TestInsert(t *testing.T) {
	tree := btree.NewBtreeSize(2, 2)
	size := 100
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		if stat := tree.Insert(rd); !stat {
			t.Fatal("Insert Failed", i)
		}
	}
}
func TestSearch(t *testing.T) {
	tree := btree.NewBtreeSize(2, 3)
	size := 100
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		tree.Insert(rd)
	}
	for i := 0; i < size; i++ {
		if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i) {
			t.Fatal("Find Failed", i)
		}
	}
}
func TestUpdate(t *testing.T) {
	tree := btree.NewBtreeSize(3, 2)
	size := 100
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

func TestMarshal(t *testing.T) {
	tree := btree.NewBtreeSize(3, 3)
	size := 100
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		tree.Insert(rd)
	}
	if err := tree.Marshal("treedump1"); err != nil {
		t.Fatal(err)
	}
}
func TestUnmarshal(t *testing.T) {
	size := 100
	tree, err := btree.Unmarshal("treedump1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < size; i++ {
		if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i) {
			t.Fatal("Find Failed", i)
		}
	}
}
func BenchmarkInsert(t *testing.B) {
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
	tree.Marshal("treedump")
}
func BenchmarkSearch(t *testing.B) {
	size := 100000
	if tree, err := btree.Unmarshal("treedump"); err == nil {
		for i := 0; i < size; i++ {
			if string(tree.Search([]byte(strconv.Itoa(i)))) != strconv.Itoa(i) {
				t.Fatal("Find Failed", i)
			}
		}
	}
}
func BenchmarkUpdate(t *testing.B) {
	size := 100000
	if tree, err := btree.Unmarshal("treedump"); err == nil {
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
func BenchmarkDelete(t *testing.B) {
	size := 100000
	if tree, err := btree.Unmarshal("treedump"); err == nil {
		for i := 0; i < size; i++ {
			if stat := tree.Delete([]byte(strconv.Itoa(i))); !stat {
				t.Fatal("delete Failed", i)
			}
		}
	}
}
