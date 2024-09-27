package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	// "go/types"
	"strconv"
	"strings"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

func (t DBType) String() string {
	switch t {
	case IntType:
		return "int"
	case StringType:
		return "string"
	}
	return "unknown"
}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// TODO: some code goes here
	//check length
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}

	//check field objects are equal
	for i := range d1.Fields {
		if d1.Fields[i] != d2.Fields[i] {
			return false
		}
	}

	return true
}

// Hint: heap_page need function there:  
func (desc *TupleDesc) bytesPerTuple() int{
	//TODO
	return 0
}

func bytesPerTuple() int {
	//TODO
	return 0
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	tdCopy := &TupleDesc{
		Fields: make([]FieldType, len(td.Fields)), // Initialize Fields slice
	}
	copy(tdCopy.Fields, td.Fields)
	return tdCopy //return the pointer of the copy of td
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	//create a new pointer point to the merged
	merged := &TupleDesc{
		Fields: make([]FieldType, len(desc.Fields)+len(desc2.Fields)),
	}
	// Append fields from the first TupleDesc
	merged.Fields = append(merged.Fields, desc.Fields...)

	// Append fields from the second TupleDesc
	merged.Fields = append(merged.Fields, desc2.Fields...)

	return merged // Return the merged TupleDesc
}

// ================== Tuple Methods ======================

// Interface for tuple field values
type DBValue interface {
	EvalPred(v DBValue, op BoolOp) bool
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
    // This interface is empty, meaning any type can implement it.
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here
	// Ensure the buffer has enough capacity
	requiredCapacity := 0
	for _, field := range t.Desc.Fields {
		switch field.Ftype {
		case IntType:
			requiredCapacity += 4 // Size of int32
		case StringType:
			requiredCapacity += StringLength // Fixed size for string
		}
	}

	if b.Len()+requiredCapacity > b.Cap() {
		return fmt.Errorf("buffer has insufficient capacity")
	}

	for i, field := range t.Desc.Fields {
		switch field.Ftype {
		case IntType:
			intField, ok := t.Fields[i].(IntField)
			if !ok {
				return fmt.Errorf("expected IntField for field %d", i)
			}
			if err := binary.Write(b, binary.LittleEndian, intField.Value); err != nil {
				return fmt.Errorf("int write fail")
			}
		case StringType:
			strField, ok := t.Fields[i].(StringField)
			if !ok {
				return fmt.Errorf("expected StringField for field %d", i)
			}
			padded := make([]byte, StringLength) // StringLength defined elsewhere
			copy(padded, []byte(strField.Value)) // Copy the string bytes into the padded slice
			if err := binary.Write(b, binary.LittleEndian, padded); err != nil {
				return fmt.Errorf("string write fail")
			}
		default:
			return fmt.Errorf("unknown type")
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	tuple := &Tuple{
		Desc:   *desc,
		Fields: make([]DBValue, len(desc.Fields)),
		Rid:    RecordIDImpl{},
	}
	for i, field := range desc.Fields {
		switch field.Ftype {
		case IntType:
			var intValue int64
			if err := binary.Read(b, binary.LittleEndian, &intValue); err != nil {
				return nil, fmt.Errorf("intField read fail")
			}
			tuple.Fields[i] = IntField{Value: intValue}
		case StringType:
			var padded [StringLength]byte
			if err := binary.Read(b, binary.LittleEndian, &padded); err != nil {
				return nil, fmt.Errorf("strField read fail")
			}
			strValue := string(bytes.TrimRight(padded[:], "\x00"))
			tuple.Fields[i] = StringField{Value: strValue}
		}
	}
	return tuple, nil //replace me
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}
	for i, field := range t1.Fields {
		if field != t2.Fields[i] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2
// appended to t1. The new tuple should have a correct TupleDesc that is created
// by merging the descriptions of the two input tuples.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	t3 := &Tuple{
		Desc: TupleDesc{
			Fields: append(t1.Desc.Fields, t2.Desc.Fields...), //apend 2 slices together
		},
		Fields: append(t1.Fields, t2.Fields...),
		Rid:    RecordIDImpl{},
	}
	return t3 //replace me
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
//
// Note that EvalExpr uses the [Tuple.project] method, so you will need
// to implement projection before testing compareField.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	// Evaluate the expression for the first tuple
	val1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err 
	}
	// Evaluate the expression for the second tuple
	val2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err 
	}

	switch v1 := val1.(type){
		case IntField:
			if v2, ok := val2.(IntField); ok{
				if v1.Value < v2.Value {
					return OrderedLessThan, nil			
				} else if v1.Value > v2.Value{
					return OrderedGreaterThan, nil
				}
				return OrderedEqual, nil
			}
		case StringField:
			if v2, ok := val2.(StringField); ok{
				if v1.Value < v2.Value {
					return OrderedLessThan, nil			
				} else if v1.Value > v2.Value{
					return OrderedGreaterThan, nil
				}
				return OrderedEqual, nil
			}
	}

	return OrderedEqual, fmt.Errorf("compareField not implemented") // replace me
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	// Create a map to store the selected fields for quick lookup
	selectedFields := make(map[string]FieldType)
	for _, field := range fields {
		selectedFields[field.Fname] = field // Store the field names
	}
	// keep track of the projected fields
	projectedFields := make([]DBValue, 0, len(fields))
	for i, field := range t.Desc.Fields {
		_, exist := selectedFields[field.Fname]
		if exist {
			projectedFields = append(projectedFields, t.Fields[i])
		}
	}
	t3 := &Tuple{
		Desc: TupleDesc{
			Fields: fields},
		Fields: projectedFields,
		Rid:    t.Rid,
	}
	return t3, fmt.Errorf("project not implemented") //replace me
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	var buf bytes.Buffer
	t.writeTo(&buf)
	return buf.String()
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = strconv.FormatInt(f.Value, 10)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr
}
