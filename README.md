contentstore
============

Go library for storing sha1-addressable blobs ([]byte slices)

A sample use:

    // Note: in production code, check the error codes!
    store, _ := contentstore.NewStore("mystore")
    id, _ := store.Put([]byte("my piece of content"))
    v, _ := store.Get(id)
    store.Close()

Documentation: http://godoc.org/github.com/kjk/contentstore
