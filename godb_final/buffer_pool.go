package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	// "fmt"
	"fmt"
	"sync"
	"time"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type LockType int

const (
	Shared LockType = iota
	Exclusive
)

type LockInfo struct {
	// mu     sync.Mutex                 // Mutex to synchronize access to this page's lock state
	owners map[TransactionID]LockType // Tracks which transactions hold which type of lock (shared/exclusive)
}

type BufferPool struct {
	pages            map[any]Page
	pageLocks        map[any]*LockInfo // locks for each page
	maxPages         int
	logFile          *LogFile
	pageLocksMutex   sync.Mutex              // Mutex to synchronize access to pageLocks
	transactions     map[TransactionID]bool  // Map to track running transactions
	transactionPages map[TransactionID][]any // Maps transaction ID to modified pages
	mu               sync.Mutex
	waitForGraph     WaitForGraph // Wait-for graph to detect deadlocks
}

type WaitForGraph map[TransactionID][]TransactionID

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{make(map[any]Page), make(map[any]*LockInfo), numPages, nil, sync.Mutex{}, make(map[TransactionID]bool), make(map[TransactionID][]any), sync.Mutex{}, make(WaitForGraph)}, nil
}

// Testing method -- iterate through all pages in the buffer pool and flush them
// using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.pages {
		page.getFile().flushPage(page)
	}
}

// Testing method -- flush all dirty pages in the buffer pool and set them to
// clean. Does not need to be thread/transaction safe.
// TODO: some code goes here :
// func (bp *BufferPool) flushDirtyPages(tid TransactionID) error {
// 	bp.mu.Lock()
// 	defer bp.mu.Unlock()
// 	for _, hashcode := range bp.transactionPages[tid] {
// 		bp.pages[hashcode].getFile().flushPage(bp.pages[hashcode])
// 		bp.pages[hashcode].setDirty(tid, false)
// 	}
// 	return nil
// }

func (bp *BufferPool) flushDirtyPages(tid TransactionID) error {
	// Check if there are pages for this transaction
	pagesToFlush, exists := bp.transactionPages[tid]
	if !exists || len(pagesToFlush) == 0 {
		return fmt.Errorf("no pages found for transaction %v", tid)
	}

	// Loop over pages associated with the transaction
	for _, hashcode := range pagesToFlush {
		// Check if the page exists in the buffer pool
		page, pageExists := bp.pages[hashcode]
		if !pageExists {
			return fmt.Errorf("page with hashcode %v not found in buffer pool", hashcode)
		}

		// Check if the page's file is nil
		pageFile := page.getFile()
		if pageFile == nil {
			return fmt.Errorf("page with hashcode %v has a nil file, cannot flush", hashcode)
		}

		// Attempt to flush the page
		err := pageFile.flushPage(page)
		if err != nil {
			return fmt.Errorf("failed to flush page with hashcode %v: %v", hashcode, err)
		}

		// Set the page as not dirty
		page.setDirty(tid, false)
	}

	return nil
}

// Returns true if the transaction is runing.
//
// Caller must hold the bufferpool lock.
// TODO: some code goes here :
func (bp *BufferPool) tidIsRunning(tid TransactionID) bool {
	bp.mu.Lock()         // Lock to protect shared state like bp.transactions
	defer bp.mu.Unlock() // Ensure the lock is released when function exits

	return bp.transactions[tid]
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
// TODO: some code goes here : func (bp *BufferPool) AbortTransaction(tid TransactionID)
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.mu.Lock() // Lock to protect global state (transactions, transactionPages)
	defer bp.mu.Unlock()
	//fmt.Println("abort transaction", tid, "remove from the graph", bp.waitForGraph)

	// Release locks for this transaction
	for _, hashcode := range bp.transactionPages[tid] {
		bp.pageLocksMutex.Lock()
		delete(bp.pageLocks[hashcode].owners, tid)
		//fmt.Println("delete", tid, "from page", hashcode, "owners", bp.pageLocks[hashcode].owners)
		bp.pageLocksMutex.Unlock()
	}
	//delete the pages associated with this transaction from buffer pool to abort
	for _, hashcode := range bp.transactionPages[tid] {
		delete(bp.pages, hashcode)
	}

	//delete transaction from transactions and transactionPages
	delete(bp.transactions, tid)
	delete(bp.transactionPages, tid)

	//remove tid from the wait-for graph
	bp.removeFromWaitForGraph(tid)

	//fmt.Println("transactions", bp.transactions, "transactionPages", bp.transactionPages, "waitForGraph", bp.waitForGraph)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
// TODO: some code goes here : func (bp *BufferPool) CommitTransaction(tid TransactionID)
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.mu.Lock() // Lock to protect global state (transactions, transactionPages)
	defer bp.mu.Unlock()

	// Flush dirty pages associated with this transaction to disk
	err := bp.flushDirtyPages(tid)
	if err != nil {
		fmt.Errorf("error flushing dirty pages for transaction %d: %v", tid, err)
	}
	// Release locks for this transaction
	for _, hashcode := range bp.transactionPages[tid] {
		bp.pageLocksMutex.Lock()
		delete(bp.pageLocks[hashcode].owners, tid)
		bp.pageLocksMutex.Unlock()
	}

	//delete transaction from transactions and transactionPages
	delete(bp.transactions, tid)
	delete(bp.transactionPages, tid)

	//remove tid from the wait-for graph
	bp.removeFromWaitForGraph(tid)

	//fmt.Println("comitted transaction", tid, "remove from the graph", bp.waitForGraph)
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
// TODO: some code goes here: func (bp *BufferPool) BeginTransaction(tid TransactionID) error
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	//check if the transaction is already running
	bp.mu.Lock() // Lock access to the transactions map to ensure thread safety
	defer bp.mu.Unlock()
	// Return an error if the transaction exists
	if _, exists := bp.transactions[tid]; exists {
		return fmt.Errorf("transaction %d is already running", tid) // Return an error if the transaction exists
	}
	// If not, add the transaction to the map and mark it as running
	bp.transactions[tid] = true
	return nil
}

// If necessary, evict clean page from the buffer pool. If all pages are dirty,
// return an error.
func (bp *BufferPool) evictPage() error {
	if len(bp.pages) < bp.maxPages {
		return nil
	}

	// evict first clean page
	for key, page := range bp.pages {
		if !page.isDirty() {
			delete(bp.pages, key)
			return nil
		}
	}

	return GoDBError{BufferPoolFullError, "all pages in buffer pool are dirty"}
}

// Returns true if the transaction is runing.
// TODO: some code goes here :
func (bp *BufferPool) IsRunning(tid TransactionID) bool {
	bp.mu.Lock()         // Lock to protect shared state like bp.transactions
	defer bp.mu.Unlock() // Ensure the lock is released when function exits

	return bp.transactions[tid]
}

// Loads the specified page from the specified DBFile, but does not lock it.
// TODO: some code goes here :
func (bp *BufferPool) loadPage(file DBFile, pageNo int) (Page, error) {
	hashCode := file.pageKey(pageNo)
	pg, ok := bp.pages[hashCode]
	if !ok {
		err := bp.evictPage()
		if err != nil {
			return nil, err
		}
		pg, err = file.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		bp.pages[hashCode] = pg
	}
	return pg, nil
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

	bp.mu.Lock() // Lock for pages map (bp.pages)
	hashCode := file.pageKey(pageNo)
	pg, ok := bp.pages[hashCode]
	if !ok {
		err := bp.evictPage()
		if err != nil {
			bp.mu.Unlock() // Make sure to unlock at the end of the function
			return nil, err
		}
		pg, err = file.readPage(pageNo)
		if err != nil {
			bp.mu.Unlock() // Make sure to unlock at the end of the function
			return nil, err
		}
		bp.pages[hashCode] = pg
	}
	bp.mu.Unlock() // Make sure to unlock at the end of the function

	// Ensure that pageLocks[hashCode] is initialized
	bp.pageLocksMutex.Lock()
	if _, exists := bp.pageLocks[hashCode]; !exists {
		bp.pageLocks[hashCode] = &LockInfo{
			owners: make(map[TransactionID]LockType),
		}
		//fmt.Println("initialize pageLocks")
	}
	bp.pageLocksMutex.Unlock()

	bp.mu.Lock() // Lock to protect shared state like bp.transactions
	bp.transactionPages[tid] = append(bp.transactionPages[tid], hashCode)
	bp.mu.Unlock() // Ensure the lock is released when function exits

	//fmt.Println("attempt to get locks")
	//get lock for page according to perm, keep trying until lock is available
	if perm == ReadPerm {
		//fmt.Println("tid", tid, "attempt to get s-lock at page", pageNo)
		for {
			//get mutex for pageLocks
			bp.pageLocksMutex.Lock()
			//aquire shared lock if no owner or only shared locks are present
			if len(bp.pageLocks[hashCode].owners) == 0 { // if there is no owner, aquire shared lock imeediately
				bp.pageLocks[hashCode].owners[tid] = Shared
				bp.pageLocksMutex.Unlock() //unlock pageLocks so that other transactions can get locks or release locks
				//fmt.Println("tid", tid, "accquired s-lock at page", pageNo)
				break
			} else {
				//iterate owners to check if there is any Exclusive lock on the page, otherwise, can just aquire shared lock
				hasExclusive := false
				for t, lockType := range bp.pageLocks[hashCode].owners {
					if lockType == Exclusive && t != tid {
						hasExclusive = true
						//add tid as blocking tid to the wait-for graph
						bp.mu.Lock()
						bp.addToWaitForGraph(tid, t)
						bp.mu.Unlock()
						// after adding to the wait-for graph, check if dead lock occurs
						isDeadLock := bp.detectCycle(tid)
						if isDeadLock {
							bp.pageLocksMutex.Unlock() //unlock pageLocks
							// Abort the transaction that is causing the deadlock
							bp.AbortTransaction(tid)
							return nil, fmt.Errorf("deadlock detected for transaction %d", tid)
						}
						//fmt.Println("add", tid, "to wait-for graph, blocking by", t)
						break
					}
				}
				if hasExclusive {
					bp.pageLocksMutex.Unlock() //unlock pageLocks so that other transactions can get locks or release locks
					//fmt.Println("retry to get s-lock")
					time.Sleep(time.Millisecond * 100) //sleep for 100ms before trying again
					continue
				} else {
					bp.pageLocks[hashCode].owners[tid] = Shared
					bp.pageLocksMutex.Unlock() //unlock pageLocks so that other transactions can get locks or release locks
					break
				}
			}
		}
	} else {
		//fmt.Println("tid", tid, "attempt to get x-lock at page", pageNo)
		for {
			//get mutex for pageLocks
			//fmt.Println("tid", tid, "attempt to get x-lock at page", pageNo, "owners", bp.pageLocks[hashCode].owners, bp.pageLocks[hashCode].owners[tid])
			bp.pageLocksMutex.Lock()

			// Check if the current tid exists in the map and if the map is empty
			currentLock, exists := bp.pageLocks[hashCode].owners[tid]

			if len(bp.pageLocks[hashCode].owners) == 0 || (len(bp.pageLocks[hashCode].owners) == 1 && exists && currentLock == Shared) {
				bp.pageLocks[hashCode].owners[tid] = Exclusive
				bp.pageLocksMutex.Unlock() //unlock pageLocks so that other transactions can get locks or release locks
				//fmt.Println("tid", tid, "accquired x-lock at page", pageNo, "owners", bp.pageLocks[hashCode].owners)
				break
			} else { //add tid as blocking tid to the wait-for graph
				for t, _ := range bp.pageLocks[hashCode].owners {
					if t != tid {
						bp.mu.Lock()
						bp.addToWaitForGraph(tid, t)
						bp.mu.Unlock()
						//fmt.Println("add", tid, "to wait-for graph, blocking by", t)
						// after adding to the wait-for graph, check if dead lock occurs
						isDeadLock := bp.detectCycle(tid)
						if isDeadLock {
							bp.pageLocksMutex.Unlock() //unlock pageLocks
							// Abort the transaction that is causing the deadlock
							bp.AbortTransaction(tid)
							return nil, fmt.Errorf("deadlock detected for transaction %d", tid)
						}
						break
					}
				}
			}
			bp.pageLocksMutex.Unlock()
			time.Sleep(time.Millisecond * 100) //sleep for 100ms before trying again
		}
	}
	// After acquiring the lock, remove the transaction from the wait-for graph
	//bp.removeFromWaitForGraph(tid, blockingTid)

	//after aquire lock, return the page
	return pg, nil
}

// Detects cycles in the wait-for graph using DFS
func (bp *BufferPool) detectCycle(tid TransactionID) bool {
	//fmt.Println("detect cycle for", tid)
	visited := make(map[TransactionID]bool) // To track if a transaction has been visited.
	onStack := make(map[TransactionID]bool) // To track if a transaction is in the current DFS recursion stack.
	isDeadLock := bp.dfsDetectCycle(tid, visited, onStack)
	//fmt.Println("detect cycle for", tid, "isDeadLock", isDeadLock)
	return isDeadLock
}

// Recursive DFS to detect cycles in the wait-for graph
func (bp *BufferPool) dfsDetectCycle(tid TransactionID, visited, onStack map[TransactionID]bool) bool {
	// If the transaction is already in the call stack (onStack), a cycle is detected
	//fmt.Println("dfs detect cycle for", tid, "visited: ", visited, "onStack", onStack)
	if onStack[tid] {
		return true
	}

	// If already visited, no need to process
	if visited[tid] {
		return false
	}

	// Mark current transaction as visited and part of the current DFS path
	visited[tid] = true
	onStack[tid] = true

	// Traverse all the transactions this tid is waiting on
	for _, blockedTid := range bp.waitForGraph[tid] {
		if bp.dfsDetectCycle(blockedTid, visited, onStack) {
			return true
		}
	}

	// Unmark the transaction as part of the current DFS path after each recursive call
	onStack[tid] = false
	return false
}

func (bp *BufferPool) addToWaitForGraph(waitingTid, blockedTid TransactionID) {
	// fmt.Println("wait-for graph", bp.waitForGraph)
	// Check if the blockedTid is already in the wait-for list of waitingTid
	if _, exists := bp.waitForGraph[waitingTid]; !exists {
		// If the waitingTid doesn't exist in the graph, initialize the list
		bp.waitForGraph[waitingTid] = []TransactionID{blockedTid}
		//fmt.Println("add", waitingTid, "to wait-for graph, waiting for", blockedTid, bp.waitForGraph)
	} else { // add blockedTid to the existing list of transactions waiting on waitingTid
		// Check if the blockedTid is already in the list to avoid duplicates
		for _, t := range bp.waitForGraph[waitingTid] {
			if t == blockedTid {
				return // blockedTid is already in the list, so do nothing
			}
		}
		//fmt.Println("add", waitingTid, "to wait-for graph, waiting for", blockedTid)
		bp.waitForGraph[waitingTid] = append(bp.waitForGraph[waitingTid], blockedTid)
	}
}

func (bp *BufferPool) removeFromWaitForGraph(Tid TransactionID) {
	//remove Tid itself from waitForGraph
	delete(bp.waitForGraph, Tid)
	//remove Tid from the list of transactions waiting on Tid
	for tid, blockedTids := range bp.waitForGraph {
		for i, blockedTid := range blockedTids {
			if blockedTid == Tid {
				bp.waitForGraph[tid] = append(bp.waitForGraph[tid][:i], bp.waitForGraph[tid][i+1:]...)
				break
			}
		}
	}
}
