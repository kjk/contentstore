package contentstore

import (
	"math/rand"
	"os"
	"testing"

	"github.com/kjk/u"
)

func removeStoreFiles(basePath string) {
	path := idxFilePath(basePath)
	os.Remove(path)
	nSegment := 0
	for {
		path = segmentFilePath(basePath, nSegment)
		if !u.PathExists(path) {
			break
		}
		os.Remove(path)
		nSegment += 1
	}
}

func genRandBytes(rnd *rand.Rand, n int) []byte {
	res := make([]byte, n, n)
	for n > 0 {
		b := byte(rnd.Intn(256))
		res[n-1] = b
	}
	return res
}

func TestStore(t *testing.T) {
	SEGMENT_MAX_SIZE := 100 * 1024 // 100 kB
	SMALL_BLOB_MAX := 10 * 1024    // 10 kB
	BIG_BLOB_MAX := 200 * 1024     // 200 kB
	// initialize with known source to get predictable results
	rnd := rand.New(rand.NewSource(0))
	basePath := "test"
	removeStoreFiles(basePath)
	store, err := NewStoreWithLimit(basePath, SEGMENT_MAX_SIZE)
	if err != nil {
		t.Fatalf("NewStoreWithLimit(%q, %d) failed with %q", basePath, SEGMENT_MAX_SIZE, err)
	}
	defer store.Close()
	nWritten := 0
	blobIds := make([]string, 0)
	for nWritten < 20*1024*1024 {
		var d []byte
		isBig := (rnd.Intn(10) == 0) // 10% of the time generate big blob
		if isBig {
			d = genRandBytes(rnd, BIG_BLOB_MAX)
		} else {
			d = genRandBytes(rnd, SMALL_BLOB_MAX)
		}
		id, err := store.Put(d)
		if err != nil {
			t.Fatalf("store.Put() failed with %q", err)
		}
		blobIds = append(blobIds, id)
		nWritten += len(d)
		// TODO: test that created segments as expected
	}
	var d []byte
	nBlobs := len(blobIds)
	for i := 0; i < nBlobs; i++ {
		n := rnd.Intn(nBlobs)
		id := blobIds[n]
		d, err = store.Get(id)
		if err != nil {
			t.Fatalf("store.Get(%q) failed with %q", id, err)
		}
		sha1 := string(u.Sha1OfBytes(d))
		if sha1 != id {
			t.Fatalf("store.Get() returned bad content, id is %x while sha1 is %x, should be same", []byte(id), []byte(sha1))
		}
	}
	k := "non-existint"
	d, err = store.Get(k)
	if err == nil {
		t.Fatalf("store.Get(%q) returned nil err, expected an error", k)
	}
	// TODO: re-open and re-test existence
	// TODO: re-open and add new items
}
