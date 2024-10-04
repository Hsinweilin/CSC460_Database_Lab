package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	numPages int
	pages map[any]Page 
	logFile *LogFile
	// order []int // keep track of accessing order
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	// TODO: some code goes here
	buffPool := &BufferPool{
		numPages: numPages,
		pages: make(map[any]Page),
		// order: make([]int, 0, numPages),//use to keep track of least accessed page
		logFile: nil,// fix this in future lab
	}
	return buffPool, nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	for key, page := range bp.pages{
		dbFile := page.getFile()
		if err := dbFile.flushPage(page); err != nil{
			fmt.Println("Error flushing page:", err, key)
			page.setDirty(0, false)
		}
	}
}


// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	fmt.Println("bp.numPages, bp.pages, pageNo", bp.numPages, len(bp.pages), pageNo)
	if pageNo > file.NumPages(){
		return nil, fmt.Errorf("pageNo out of range")
	}
	hashCode := file.pageKey(pageNo)
	pg, ok := bp.pages[hashCode]
	//fmt.Println(pg)
	if !ok{
		if len(bp.pages) >= bp.numPages{//check if bufferpool is full
			err := bp.evictPage()
			if err != nil{
				return nil, err
			}
		}
		//fmt.Println("reading from heapfile")
		pg, err := file.readPage(pageNo)
		if err != nil{
			return nil, err
		}
		bp.pages[hashCode] = pg
		return pg, nil
	}
	return pg, nil
	// // Check if page number is within valid range
	// if pageNo < 0 || pageNo >= bp.numPages {
	// 	return nil, fmt.Errorf("invalid page number: %d", pageNo)
	// }
	// // TODO: some code goes here
	// // if page already cached
	// if cached := bp.pages[pageNo]; cached != nil{
	// 	bp.updateAccessOrder(pageNo) // Update access order
	// 	return cached, nil
	// }
	
	// // if not, read from Disk, also check the size of numPage in bufferPool
	// if len(bp.pages) >= bp.numPages{// if bufferPool is full, evict a page
	// 	if err := bp.evictPage(); err != nil{
	// 		//if evict page fail, return error
	// 		return nil, err
	// 	}
	// }

	// // read file from Disk using heap file
	// page, err := file.readPage(pageNo)
	// if err != nil{
	// 	fmt.Println("Error reading file from Disk:", err)
	// 	return nil, err
	// }
	// // cache the page in the bufferPool
	// bp.pages[pageNo] = page
	// bp.order = append(bp.order, pageNo)// add newly accessed page into order slice
	// return page, nil	
}

// Hint: GetPage function need function there: func (bp *BufferPool) evictPage() error
func (bp *BufferPool) evictPage() error{
	// if len(bp.pages) < bp.numPages{
	// 	return nil
	// }
	for key, val := range bp.pages{
		if !val.isDirty(){// if not dirty, evict this page
			delete(bp.pages, key)// evict that page from bufferpool map
			return nil
		}
	}
	return fmt.Errorf("all pages in buffer pool is dirty")
}

// // keep track of least accessed page
// func (bp *BufferPool) updateAccessOrder(pageNo int) {
//     // Remove the page from its current position in the order slice
//     for i, key := range bp.order {
//         if key == pageNo {
//             bp.order = append(bp.order[:i], bp.order[i+1:]...)
//             break
//         }
//     }
//     // Add the page key to the end of the order slice
//     bp.order = append(bp.order, pageNo)
// }
