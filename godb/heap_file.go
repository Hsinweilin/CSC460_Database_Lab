package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	filename  string // Store the name of the backing file
	file *os.File
	tupleDesc *TupleDesc
	pages     []*heapPage
	numPages  int
	bufPool   *BufferPool
	sync.Mutex
}

// Hint: heap_page and heap_file need function there:  type heapFileRid struct
type heapFileRid struct {
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// Attempt to open the file
	file, err := os.OpenFile(fromFile, os.O_CREATE|os.O_WRONLY, 0644)//open or create file, just make sure there is a file exist
	if err != nil {
		return nil, fmt.Errorf("could not open or create file: %w", err)
	}
	defer file.Close() // Close the file 
	fileSize, err := file.Stat()
	if err != nil{
		return nil, err
	}
	numPages := fileSize.Size() / int64(PageSize)

	hf := &HeapFile{
		filename: fromFile,
		file:  file,
		tupleDesc: td,
		bufPool:   bp,
		pages:     []*heapPage{}, // Start with an empty slice of pages
		numPages:  int(numPages),              // Start with zero pages
	}
	//fmt.Println(hf, "numPages", hf.numPages)
	return hf, nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	// TODO: some code goes here
	return f.filename
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	return f.numPages
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {

	file, err := os.Open(f.filename) // for read-only access
    if err != nil {
        return nil, fmt.Errorf("could not open file: %w", err)
    }
	defer file.Close() // Ensure the file is closed after reading

    offset := int64(pageNo * PageSize)

	//fmt.Println("reading at offset: ", offset)
    buffer := make([]byte, PageSize)
    file.ReadAt(buffer, offset)

    byteBuffer := bytes.NewBuffer(buffer)
    hp, err := newHeapPage(f.tupleDesc, pageNo, f)
	if err != nil{
		fmt.Errorf("fail to create new heap page %w", err)
	}
    hp.initFromBuffer(byteBuffer)
	//fmt.Println(hp)
    return hp, nil
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	for i := 0; i < f.numPages; i++ {
		page, err := f.bufPool.GetPage(f, i, tid, WritePerm)
		//fmt.Println(page, i)
		if err != nil {
			return fmt.Errorf("failed to get page %d from buffer pool: %w", i, err)
		}
		hp, ok := page.(*heapPage) // Assert that page is of type *heapPage
		if !ok {
			return fmt.Errorf("page is not of type *heapPage")
		}
		if hp.getNumEmptySlots() > 0 {
			//fmt.Println("pageNo, empty slots", hp.pageNo, hp.getNumEmptySlots())
			rid, err := hp.insertTuple(t)                  //TODO
			if err != nil {
				if err.Error() == "heapPage is already full" { // This will only run if err is not nil
					t.Rid = rid // Ensure this is handled correctly
					continue
				}
				return err // Return the error if it's something else
			}
			// if successful insert, get out of the for loop, set heap Page to dirty
			hp.setDirty(tid, true)
			return nil
		}
	}
	//after iterating all heap pages, still no free space, create a new page
	//fmt.Println("create new heapPage")
	newPageNum := f.numPages
	hp, err := newHeapPage(&t.Desc, newPageNum, f)
	if err != nil{
		return fmt.Errorf("create new heapPage fail")
	}
	f.numPages ++
	_, err = hp.insertTuple(t)
	if err != nil{
		return err
	}
	f.pages = append(f.pages, hp)
	err = f.flushPage(hp)
	if err != nil{
		return err
	}
	// hp.setDirty(tid, true)
	return nil
}


// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// // Type assertion to extract the RecordIDImpl
	rid, ok := t.Rid.(RecordIDImpl)
	if !ok {
		return fmt.Errorf("provided recordID is not of type RecordIDImpl")
	}

	// Retrieve the page containing the tuple to delete
	page, err := f.bufPool.GetPage(f, rid.PageNum, tid, WritePerm)
	if err != nil {
		return fmt.Errorf("failed to read page %d in deleteTuple: %w", rid.PageNum, err)
	}

	// Assert that the retrieved page is of type *heapPage
	hp, ok := page.(*heapPage)
	if !ok {
		return fmt.Errorf("retrieved page is not of type *heapPage")
	}

	// Attempt to delete the tuple from the heapPage
	if err := hp.deleteTuple(rid); err != nil {
		return fmt.Errorf("failed to delete tuple from heapPage: %w", err)
	}

	// Mark the heap page as dirty after modification
	hp.setDirty(tid, true)
	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
// 4. `flushPage()` - Force a given page object back to disk.  The supplied page will be a `HeapPage`;  
// you should cast it and retrieve its bytes via the heap page method `toBytes()`.  
// You can then write these bytes back to the appropriate location on disk 
// by opening the backing file and using a method like `os.File.WriteAt()`.
func (f *HeapFile) flushPage(p Page) error {
	// TODO: some code goes here
	// Cast to HeapPage
	hp, ok := p.(*heapPage)
	if !ok {
		return fmt.Errorf("expected a HeapPage, got a different type")
	}

    // Get the bytes from the HeapPage
    pageBytes, err := hp.toBuffer()
	if err != nil{
		return err
	}
    
    // Calculate the offset in the file
    offset := int64(hp.pageNo * PageSize)
    
	file, err := os.OpenFile(f.filename, os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return fmt.Errorf("could not open file: %w", err)
    }
	defer file.Close() // Ensure the file is closed after reading

    // Write the bytes back to the file at the correct offset
    n, err := file.WriteAt(pageBytes.Bytes(), offset)
    if err != nil {
    	return fmt.Errorf("could not write page to disk: " + err.Error(), n)
    }
	//fmt.Println("flush page success")
    
    return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.tupleDesc

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	currentPageNo := 0
    //currentTupleIndex := 0
	var curIter func() (*Tuple, error)//get tupleIter
	curIter = nil
	//fmt.Println(len(f.bufPool.pages))
    return func() (*Tuple, error) {
		if curIter != nil {
			tup, _ := curIter()
			if tup != nil {
				return tup, nil
			}
				// Iter is done, move to
			currentPageNo++
		}
		if currentPageNo > f.numPages {
			//fmt.Println(currentPageNo, f.numPages)
			return nil, nil
		}
		page, err := f.bufPool.GetPage(f, currentPageNo, tid, ReadPerm)
		//fmt.Println(page)
		if err != nil{
			return nil, fmt.Errorf("get page in iterator fail")
		}
		if page == nil{
			return nil, fmt.Errorf("get page in iterator fail")
		}
		hp := page.(*heapPage)
		//fmt.Println(hp)
		curIter = hp.tupleIter()
		return curIter()
	}, nil
}


// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	// TODO: some code goes here
	Key := heapHash{
		FileName: f.filename,
		PageNo: pgNo,
	}
	return Key
}
