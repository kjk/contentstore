package contentstore

import (
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/kjk/u"
)

// Files we use:
// - index where for each blob we store: sha1 of the content, number of segment
//   file in which the blob is stored, offset within the segment and size of
//   the blob
// - one or more segment files. User can control the max size of segment
//   file (10 MB by default) to pick the right file size/number of files
//   balance for his needs

// Ideas for the future:
// - add a mode where we store big blobs (e.g. over 1MB) in their own files
// - add a way to delete files and re-use deleted space via some sort of
//   best-fit allocator (if we're adding a new blob and have free space
//   due to deletion, pick the free space that is the closest in size to
//   new blob but only if it's e.g. withing 10% in size, to avoid excessive
//   fragmentation)
// - add a way to delete files by rewriting the files (expensive! we have to
//   rewrite the whole index and each segment that contains deleted files)

var (
	errNotFound           = errors.New("not found")
	errInvalidIndexHdr    = errors.New("invalid index file header")
	errInvalidIndexLine   = errors.New("invalid index line")
	errSegmentFileMissing = errors.New("segment file missing")
	errNotValidSha1       = errors.New("not a valid sha1")
	// first line in index file, for additional safety
	idxHdr = "github.com/kjk/contentstore header 1.0"
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
	idxFile         *os.File
	idxCsvWriter    *csv.Writer
	currSegmentFile *os.File
	currSegment     int
	currSegmentSize int
}

func idxFilePath(baseFilePath string) string {
	return baseFilePath + "_idx.txt"
}

func segmentFilePath(baseFilePath string, nSegment int) string {
	return fmt.Sprintf("%s_%d.txt", baseFilePath, nSegment)
}

func decodeIndexLine(rec []string) (blob Blob, err error) {
	if len(rec) != 4 {
		return blob, errInvalidIndexLine
	}
	sha1, err := hex.DecodeString(rec[0])
	if err != nil {
		return blob, err
	}
	if len(sha1) != 20 {
		return blob, errNotValidSha1
	}
	copy(blob.sha1[:], sha1)
	if blob.nSegment, err = strconv.Atoi(rec[1]); err != nil {
		return blob, err
	}
	if blob.offset, err = strconv.Atoi(rec[2]); err != nil {
		return blob, err
	}
	if blob.size, err = strconv.Atoi(rec[3]); err != nil {
		return blob, err
	}
	return blob, nil
}

// appends x to array of ints
func appendIntIfNotExists(aPtr *[]int, x int) {
	a := *aPtr
	if sort.SearchInts(a, x) >= 0 {
		return
	}
	a = append(a, x)
	sort.Ints(a)
	*aPtr = a
}

func (store *Store) readExistingData() error {
	// at this point idx file must exist
	fidx, err := os.Open(idxFilePath(store.baseFilePath))
	if err != nil {
		return err
	}
	// TODO: would be faster (and easier?) to use a bitset since we know
	// segment numbers are consequitive integers
	segments := make([]int, 0)
	csvReader := csv.NewReader(fidx)
	csvReader.Comma = ','
	csvReader.FieldsPerRecord = -1
	rec, err := csvReader.Read()
	if err != nil || len(rec) != 1 || rec[0] != idxHdr {
		return errInvalidIndexHdr
	}
	for {
		rec, err = csvReader.Read()
		if err != nil {
			break
		}
		blob, err := decodeIndexLine(rec)
		if err != nil {
			break
		}
		appendIntIfNotExists(&segments, blob.nSegment)
		store.blobs = append(store.blobs, blob)
	}
	if err == io.EOF {
		err = nil
	}
	// verify segment files exist
	// TODO: also verify offset + size is <= size of segment file
	for _, nSegment := range segments {
		path := segmentFilePath(store.baseFilePath, nSegment)
		if !u.PathExists(path) {
			return errSegmentFileMissing
		}
	}
	return nil
}

func NewStoreWithLimit(baseFilePath string, maxSegmentSize int) (store *Store, err error) {
	store = &Store{
		baseFilePath:   baseFilePath,
		blobs:          make([]Blob, 0),
		maxSegmentSize: maxSegmentSize,
	}
	idxPath := idxFilePath(baseFilePath)
	idxExists := u.PathExists(idxPath)
	if idxExists {
		// Note: we have enough data in segment files to reconstruct the index
		// in the rare case that index got deleted but segment files did not.
		// The logic to do that is not implemented.
		if err = store.readExistingData(); err != nil {
			return nil, err
		}
	}

	if store.idxFile, err = os.OpenFile(idxPath, os.O_WRONLY|os.O_APPEND, 0644); err != nil {
		return nil, err
	}

	return store, nil
}

func NewStore(baseFilePath string) (*Store, error) {
	return NewStoreWithLimit(baseFilePath, 10*1024*1024)
}

func closeFilePtr(filePtr **os.File) (err error) {
	f := *filePtr
	if f != nil {
		err = f.Close()
		*filePtr = nil
	}
	return err
}

func (store *Store) Close() {
	closeFilePtr(&store.idxFile)
	closeFilePtr(&store.currSegmentFile)
}

func (store *Store) Put(d []byte) (id string, err error) {
	store.Lock()
	defer store.Unlock()

	idBytes := u.Sha1OfBytes(d)
	id = string(idBytes)
	if _, ok := store.sha1ToBlobNo[id]; ok {
		return id, nil
	}
	// TODO: save the blob to current segment
	// TODO: when exceeded segment size, switch to a new segment
	return "", errors.New("NYI")
}

// TODO: cache file descriptor
func readFromFile(path string, offset, size int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	res := make([]byte, size, size)
	if _, err := f.ReadAt(res, int64(offset)); err != nil {
		return nil, err
	}
	return res, nil
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
