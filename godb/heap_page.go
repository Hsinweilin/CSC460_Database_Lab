package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	pageNo int 
	numSlots int32
	usedSlots int32
	tuples []*Tuple
	dirty bool
	file DBFile
	nextSlot int
	tupledesc  *TupleDesc
	transactionID TransactionID
	padding      []byte // Field to hold padding bytes
	sync.Mutex
}

type RecordIDImpl struct {
    PageNum int
    SlotNum int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// TODO: some code goes here
	tupleSize := 0
	for _, field := range desc.Fields{
		switch field.Ftype {
		case IntType:
			tupleSize += int(unsafe.Sizeof(int64(0)))
		case StringType:
			tupleSize += StringLength
		}
	}
	remPageSize := PageSize - 8 // bytes after header
	numSlots := remPageSize / tupleSize //integer division will round down
	hp := &heapPage{
		pageNo:   pageNo,
		numSlots:  int32(numSlots),
		usedSlots: 0, // Initially, no slots are used
		tuples:    make([]*Tuple, 0, numSlots), // Allocate slice for tuples
		dirty:     false,
		nextSlot: 0,
		tupledesc: desc,
		transactionID: -1,
		file:      f, // Assign the provided HeapFile
	}
	// Add padding to the page
    hp.padding = make([]byte, 8) // Create 8 bytes of padding
    // Optionally, initialize the padding with zeros or specific values
    for i := range hp.padding {
        hp.padding[i] = 0 // Initialize padding to zero (or any other value)
    }
	return hp, nil
}

// Hint: heapfile/insertTuple needs function there:  func (h *heapPage) getNumEmptySlots() int
func (h *heapPage) getNumEmptySlots() int{
	return int(h.numSlots) - int(h.usedSlots)
}
func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return int(h.numSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	 // Check if the page is full before inserting
	 if h.usedSlots >= h.numSlots {
        return RecordIDImpl{}, fmt.Errorf("heapPage is already full")
    }
	t.Rid = RecordIDImpl{
		PageNum: h.pageNo,
		SlotNum: h.nextSlot,
	} //assign tuple RID
	h.nextSlot += 1
	h.tuples = append(h.tuples, t)//add tuple to the end of tuple slices
	h.usedSlots += 1//increment usedSlots
	//fmt.Println("insert tuple sucess")
	return t.Rid, nil //replace me
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	for i, t := range h.tuples{
		if t.Rid == rid{
			h.tuples = append(h.tuples[:i], h.tuples[i+1:]...)// remove such tuple from the slice
			//fmt.Println("deleted tuple", t)
			h.usedSlots -= 1
			return nil
		}
	}
	return fmt.Errorf("page is empty, delete fail")
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.dirty//replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	// TODO: some code goes here
	h.transactionID = tid
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (h *heapPage) getFile() DBFile {
	// TODO: some code goes here
	return h.file  //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// for _, tuple := range h.tuples{
	// 	fmt.Println(tuple)
	// }
	// TODO: some code goes here
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, h.numSlots); err != nil{
		return nil, fmt.Errorf("numSlots toBuffer not implemented")
	}
	if err := binary.Write(buffer, binary.LittleEndian, h.usedSlots); err != nil{
		return nil, fmt.Errorf("usedSlots toBuffer not implemented")
	}
	for _, t := range h.tuples{
		if err := t.writeTo(buffer); err != nil{
			return buffer, err
		}
	}
	// fmt.Printf("Size of buffer: %d bytes\n", buffer.Len())
	return buffer, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.LittleEndian, &h.numSlots); err != nil {
		return fmt.Errorf("reading numSlots from buffer fail")
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.usedSlots); err != nil{
		return fmt.Errorf("reading numSlots from buffer fail")
	}
	// h.tuples = make([]*Tuple, 0, h.numSlots) // Ensure tuples are allocated
	for i := 0; i < int(h.usedSlots); i++{// reading tuple from buffer
		tuple, err := readTupleFrom(buf, h.tupledesc) // Use h.desc for the TupleDesc
    	if err != nil {
        	return fmt.Errorf("reading tuple %d from buffer failed: %w", i, err) // Include index and err
    	}
    	h.tuples = append(h.tuples, tuple)
	}
	
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	currentIndex := 0
	return func() (*Tuple, error) { 
		if currentIndex >= int(p.usedSlots){//end of tuples
			return nil, nil
		}
		tuple := p.tuples[currentIndex]
		tuple.Rid = RecordIDImpl{
			PageNum: p.PageNo(),
			SlotNum: currentIndex,
		}
		currentIndex ++

		return tuple, nil
	}
}
