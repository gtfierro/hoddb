package hod

import (
	"bytes"
	"sync"
)

type relationRow struct {
	content []byte
}

var rowPool = sync.Pool{
	New: func() interface{} {
		return &relationRow{
			content: make([]byte, 64),
		}
	},
}

func newRelationRow() *relationRow {
	row := rowPool.Get().(*relationRow)
	row.content = row.content[:0]
	return row
}

func (row *relationRow) release() {
	rowPool.Put(row)
}

func (row *relationRow) copy() *relationRow {
	gr := rowPool.Get().(*relationRow)
	if len(gr.content) < len(row.content) {
		gr.content = make([]byte, len(row.content))
	}
	copy(gr.content[:], row.content[:])
	return gr
}

func (row *relationRow) addValue(pos int, value EntityKey) {
	if value.Empty() {
		return
	}
	if len(row.content) < pos*16+16 {
		nrow := make([]byte, pos*16+16)
		copy(nrow, row.content)
		row.content = nrow
	}
	copy(row.content[pos*16:], value.Bytes())
}

func (row relationRow) valueAt(pos int) EntityKey {
	var k EntityKey
	if pos*16+16 > len(row.content) {
		return k
	}
	k = EntityKeyFromBytes(row.content[pos*16 : pos*16+16])
	return k
}

func (row relationRow) equals(other relationRow) bool {
	return bytes.Equal(row.content[:], other.content[:])
}
