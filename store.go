package contentstore

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/kjk/u"
)

//

var (
	errNotFound = errors.New("not found")
)

type Blob struct {
	sha1     [20]byte
	nSegment int
	offset   int
	size     int
}

type Store struct {
	sync.Mutex
	baseFilePath   string
	maxSegmentSize int
	blobs          []Blob
	// sha1ToBlob is to quickly find
	// string is really [20]byte cast to string and int is a position within blobs array
	// Note: we could try to be a bit smarter about how we
	sha1ToBlobNo    map[string]int
	fileIdx         *os.File
	fileCurrSegment *os.File
	currSegment     int
	currSegmentSize int
}

func idxFilePath(baseFilePath string) string {
	return baseFilePath + "_idx.txt"
}

func segmentFilePath(baseFilePath string, nSegment int) string {
	return fmt.Sprintf("%s_%d.txt", baseFilePath, nSegment)
}

func (store *Store) readExistingData() error {
	/*
		fidx, err := os.Open(idxFilePath(store.baseFilePath))
		if err != nil {
			return err
		}*/
	return nil
}

func NewStoreWithLimit(baseFilePath string, maxSegmentSize int) (*Store, error) {
	store := &Store{
		baseFilePath:   baseFilePath,
		blobs:          make([]Blob, 0),
		maxSegmentSize: maxSegmentSize,
	}
	idxPath := idxFilePath(baseFilePath)
	if u.PathExists(idxPath) {
		// Note: we have enough data in segment files to reconstruct the index
		// in the rare case that index got deleted but segment files did not.
		// The logic to do that is not implemented.
		store.readExistingData()
	}

	return store, nil
}

func NewStore(baseFilePath string) (*Store, error) {
	return NewStoreWithLimit(baseFilePath, 10*1024*1024)
}

func (store *Store) Put(d []byte) (id string, err error) {
	store.Lock()
	defer store.Unlock()

	idBytes := u.Sha1OfBytes(d)
	id = string(idBytes)
	if _, ok := store.sha1ToBlobNo[id]; ok {
		return id, nil
	}
	// TODO: save the blob to a file
	return "", errors.New("NYI")
}

// TODO: cache the file descriptor
func readFromFile(path string, offset, size int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	res := make([]byte, size, size)
	if _, err := f.ReadAt(res, int64(offset)); err != nil {
		return nil, err
	} else {
		return res, nil
	}
}

func (store *Store) Get(id string) ([]byte, error) {
	store.Lock()
	defer store.Unlock()
	blobNo, ok := store.sha1ToBlobNo[id]
	if !ok {
		return nil, errNotFound
	}
	blob := store.blobs[blobNo]
	path := segmentFilePath(store.baseFilePath, blob.nSegment)
	return readFromFile(path, blob.offset, blob.size)
}
