package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var write = []byte("hello world")
var width = uint64(len(write)) + lendWidth

func TestStoreAppendRead(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)

	defer os.Remove(file.Name())
	store, err := newStore(file)
	require.NoError(t, err)
	testAppend(t, store)
	testRead(t, store)
	testReadAt(t, store)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, offset := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lendWidth)
		n, err := s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, lendWidth, n)
		offset += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		offset += int64(n)
	}
}
