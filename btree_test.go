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
		if err := tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal("Insert Failed", i, err)
		}
	}
}

func TestSearch(t *testing.T) {
	tree := btree.NewBtreeSize(2, 3)
	size := 100
	for i := 0; i < size; i++ {
		tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	for i := 0; i < size; i++ {
		rst, err := tree.Search([]byte(strconv.Itoa(i)))
		if string(rst) != strconv.Itoa(i) {
			t.Fatal("Find Failed", i, err)
		}
	}
}
func TestUpdate(t *testing.T) {
	tree := btree.NewBtreeSize(3, 2)
	size := 100
	for i := 0; i < size; i++ {
		tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	for i := 0; i < size; i++ {
		if err := tree.Update([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i+1))); err != nil {
			t.Fatal("Update Failed", i, err)
		}
	}
	for i := 0; i < size; i++ {
		rst, _ := tree.Search([]byte(strconv.Itoa(i)))
		if string(rst) != strconv.Itoa(i+1) {
			t.Fatal("Find Failed", i)
		}
	}
}
func TestDelete(t *testing.T) {
	tree := btree.NewBtreeSize(3, 3)
	size := 8
	for i := 0; i < size; i++ {
		tree.Insert([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	for i := 0; i < size; i++ {
		if err := tree.Delete([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal("delete Failed", i)
		}
		if _, err := tree.Search([]byte(strconv.Itoa(i))); err == nil {
			t.Fatal("Find Failed", i)
		}
	}
}

func BenchmarkInsert(t *testing.B) {
	size := 100000
	tree := btree.NewBtree()
	for i := 0; i < size; i++ {
		tree.Insert([]byte(strconv.Itoa(i)),[]byte(strconv.Itoa(i)))
	}
}
/*
func BenchmarkSearch(t *testing.B) {
}
func BenchmarkUpdate(t *testing.B) {
}
func BenchmarkDelete(t *testing.B) {
}
*/
