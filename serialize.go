package btree


import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"io"
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
	_, err = fb.Write(append(encodefixed32(uint64(len(data))), data...))
	if err != nil {
		return err
	}
	return fb.Flush()
}

// Unmarshal btree from disk
func Unmarshal(filename string) (*Btree, error) {
	tree := &Btree {
		dupnodelist: make(map[int64]int),
		opChan:      make(chan *treeOperation),
	}
	fd, err := os.Open(filename)
	if err != nil {
		return tree, err
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)
	buf, err := readBuf(4, reader)
	if err != nil {
		return tree, err
	}
	dataLength := int(decodefixed32(buf))
	dataRecord, err := readBuf(dataLength, reader)
	if err != nil {
		return tree, err
	}
	tree.BtreeMetadata = BtreeMetadata{}
	proto.Unmarshal(dataRecord, &tree.BtreeMetadata)
	go tree.run()
	return tree, err
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
