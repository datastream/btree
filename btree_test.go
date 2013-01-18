package btree_test

import (
	"../btree"
	"strconv"
	"testing"
)

func TestInsert(t *testing.T) {
	tree := btree.NewBtreeSize(2, 2)
	size := 100
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
func TestSearch(t *testing.T) {
	tree := btree.NewBtreeSize(2, 3)
	size := 100000
	rst := make(chan bool)
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd, rst)
		<-rst
	}
	for i := 0; i < size; i++ {
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if string(rst) != strconv.Itoa(i) {
			t.Fatal("Find Failed", i)
		}
	}
}
func TestUpdate(t *testing.T) {
	tree := btree.NewBtreeSize(3, 2)
	size := 100000
	rst := make(chan bool)
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd, rst)
		<-rst
	}
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
	for i := 0; i < size; i++ {
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if string(rst) != strconv.Itoa(i+1) {
			t.Fatal("Find Failed", i)
		}
	}
}
func TestDelete(t *testing.T) {
	tree := btree.NewBtreeSize(3, 3)
	size := 100
	rst := make(chan bool)
	for i := 0; i < size; i++ {
		rd := &btree.Record{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		go tree.Insert(rd, rst)
		<-rst
	}
	for i := 0; i < size; i++ {
		d_rst := make(chan bool)
		go tree.Delete([]byte(strconv.Itoa(i)), d_rst)
		stat := <-d_rst
		if !stat {
			t.Fatal("delete Failed", i)
		}
		q_rst := make(chan []byte)
		go tree.Search([]byte(strconv.Itoa(i)), q_rst)
		rst := <-q_rst
		if rst != nil {
			t.Fatal("delete error", t)
		}
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
