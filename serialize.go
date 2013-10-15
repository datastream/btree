package btree

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"io"
	"log"
	"os"
	"strconv"
)

func (t *Btree) Dump(filename string) error {
	t.Lock()
	t.isSyning = true
	snapversion := t.GetVersion()
	size := len(t.nodes)
	fd, err := os.OpenFile(filename+"_"+strconv.Itoa(int(snapversion)), os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	defer fd.Close()
	if err != nil {
		log.Fatal("file open failed ", filename, "version ", snapversion, err)
		return err
	}
	fb := bufio.NewWriter(fd)
	data, err := proto.Marshal(&t.BtreeMetaData)
	t.Unlock()
	if err != nil {
		log.Fatal("encode tree info error ", err)
	} else {
		fb.Write(encodefixed32(uint64(len(data))))
		if _, err = fb.Write(data); err != nil {
			log.Fatal("write file error", err, "at version", snapversion)
			return err
		}
	}
	for i := 0; i < size; i++ {
		if leaf, ok := t.nodes[i].(*Leaf); ok {
			if leaf.GetVersion() <= snapversion {
				if data, err := serializeLeaf(leaf); err != nil {
					log.Fatal("encode error ", i, err)
					return err
				} else {
					fb.Write(encodefixed32(uint64(LEAF)))
					if _, err = fb.Write(data); err != nil {
						log.Fatal("write file error", err, "at version", snapversion)
						return err
					}
				}
			}
		}
		if node, ok := t.nodes[i].(*Node); ok {
			if node.GetVersion() <= snapversion {
				if data, err := serializeNode(node); err != nil {
					log.Fatal("encode error ", i, err)
				} else {
					fb.Write(encodefixed32(uint64(NODE)))
					if _, err = fb.Write(data); err != nil {
						log.Fatal("write file error", err, "at version", snapversion)
						return err
					}
				}
			}
		}
	}
	if err = fb.Flush(); err != nil {
		log.Fatal("file flush failed ", filename, "version ", snapversion, err)
		return err
	}
	go t.gc()
	t.Lock()
	t.isSyning = false
	t.Unlock()
	return nil
}

func serializeLeaf(leaf *Leaf) ([]byte, error) {
	var rst []byte
	if data, err := proto.Marshal(&leaf.IndexMetaData); err != nil {
		return rst, err
	} else {
		rst = append(rst, encodefixed32(uint64(len(data)))...)
		rst = append(rst, data...)
	}
	if data, err := proto.Marshal(&leaf.LeafRecordMetaData); err != nil {
		return rst, err
	} else {
		rst = append(rst, encodefixed32(uint64(len(data)))...)
		rst = append(rst, data...)
	}
	return rst, nil
}

func serializeNode(node *Node) ([]byte, error) {
	var rst []byte
	if data, err := proto.Marshal(&node.IndexMetaData); err != nil {
		return rst, err
	} else {
		rst = append(rst, encodefixed32(uint64(len(data)))...)
		rst = append(rst, data...)
	}
	if data, err := proto.Marshal(&node.NodeRecordMetaData); err != nil {
		return rst, err
	} else {
		rst = append(rst, encodefixed32(uint64(len(data)))...)
		rst = append(rst, data...)
	}
	return rst, nil
}

func Restore(filename string) (tree *Btree, err error) {
	fd, err := os.Open(filename)
	defer fd.Close()
	if err != nil {
		log.Fatal("file open failed ", filename, err)
		return
	}
	tree = new(Btree)
	tree.nodes = make([]TreeNode, SIZE)
	tree.isSyning = false
	reader := bufio.NewReader(fd)
	buf, err := readBuf(4, reader)
	if err != nil {
		return nil, err
	}
	dataLength := int(decodefixed32(buf))
	dataRecord, errs := readBuf(dataLength, reader)
	if errs != nil {
		return nil, errs
	}
	tree.BtreeMetaData = BtreeMetaData{}
	proto.Unmarshal(dataRecord, &tree.BtreeMetaData)
	tree.nodes = make([]TreeNode, tree.GetSize())
	for {
		// typepart
		buf, err = readBuf(4, reader)
		dataType := int(decodefixed32(buf))
		if err != nil {
			break
		}
		// get index
		buf, err = readBuf(4, reader)
		if err != nil {
			break
		}
		dataLength = int(decodefixed32(buf))
		dataRecord, err = readBuf(dataLength, reader)
		if err != nil {
			break
		}
		// get data
		buf, err = readBuf(4, reader)
		if err != nil {
			break
		}
		dataLength = int(decodefixed32(buf))
		dataRecord2, er2 := readBuf(dataLength, reader)
		if er2 != nil {
			err = er2
			break
		}
		switch dataType {
		case NODE:
			{
				node := new(Node)
				proto.Unmarshal(dataRecord, &node.IndexMetaData)
				proto.Unmarshal(dataRecord2, &node.NodeRecordMetaData)
				tree.nodes[node.GetId()] = node
			}
		case LEAF:
			{
				leaf := new(Leaf)
				proto.Unmarshal(dataRecord, &leaf.IndexMetaData)
				proto.Unmarshal(dataRecord2, &leaf.LeafRecordMetaData)
				tree.nodes[leaf.GetId()] = leaf
			}
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
}

func readBuf(dataLength int, reader *bufio.Reader) ([]byte, error) {
	dataRecord := make([]byte, dataLength)
	index := 0
	var err error
	for {
		var size int
		if size, err = reader.Read(dataRecord[index:]); err != nil {
			if err == io.EOF {
				break
			}
			log.Println("read socket data failed", err, "read size:", size, "dataLength:", dataLength)
			return nil, err
		}
		index += size
		if index == dataLength {
			break
		}
	}
	return dataRecord, err
}

func encodefixed32(x uint64) []byte {
	var p []byte
	p = append(p,
		uint8(x),
		uint8(x>>8),
		uint8(x>>16),
		uint8(x>>24))
	return p
}
func decodefixed32(num []byte) (x uint64) {
	x = uint64(num[0])
	x |= uint64(num[1]) << 8
	x |= uint64(num[2]) << 16
	x |= uint64(num[3]) << 24
	return
}
