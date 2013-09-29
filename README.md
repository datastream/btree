# Btree library

This is pure golang btree library. it's copy on write btree.

```
go get github.com/datastream/btree
```

## API

### NewRecord(key, value []byte)

create a record

### NewBtree()

create a btree

    LEAFSIZE = 1 << 5
    NODESIZE = 1 << 6

### NewBtreeSize(leafsize, nodesize)

create new btree with custom leafsize/nodesize

### btree.Insert(record)

Insert a record, if insert success, it return true.

### btree.Update(record)

Update a record, if update success, it return true

### btree.Delete(key)

Delete a record, if delete success, it return true

### btree.Search(key)

Search a key, if find success, it return value

### btree.Dump(filename)

Write btree data into disk.

    # tree.Version = 100
    tree.Dump("treedump")
    # filename should be "treedump_100"

### btree.Restore(filename)

Read btree from disk

## TODO

1. more test
2. code tunning for performance or mem
