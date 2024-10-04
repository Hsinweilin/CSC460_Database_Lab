package godb

import (
	"fmt"
	"os"
)

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (int, error) {
	// TODO: some code goes here
	hf, err := NewHeapFile(".//query1", &td, bp)
	if err != nil{
		return -1, err
	}
	csv, err := os.Open(fileName) // for read-only access
    if err != nil {
        return -1, fmt.Errorf("could not open file: %w", err)
    }
	defer csv.Close()
	err = hf.LoadFromCSV(csv, true, ",", false)
	if err != nil{
		return -1, err
	}

	sum := 0
	fieldIndex := -1

	for i, field := range td.Fields{
		fmt.Println(field.Fname)
		if field.Fname == "age"{
			fieldIndex = i
			break
		}
	}
	iter, err := hf.Iterator(0)
	if err != nil {
		return -1, err
	}

	for {
		tuple, err := iter()
		if err != nil {
			return 0, err
		}
		if tuple == nil {
			break // End of tuples
		}

		if intField, ok := tuple.Fields[fieldIndex].(IntField); ok {
			sum += int(intField.Value) // Assuming IntField has a Value field of type int64
		} else {
			return 0, fmt.Errorf("field is not a integer")
		}
	}
	return sum, nil
}
