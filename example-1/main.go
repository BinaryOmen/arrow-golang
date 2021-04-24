package main

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {

	// allocate memory
	pool := memory.NewGoAllocator()

	// define data type, data type are primitive and nested
	// struct is a nested data type.
	// struct type holds
	// 1. fields
	// 2. index
	// 3. metadata
	// Fields consist of
	// 1. Name
	// 2. DataType
	// 3. Null
	// 4. Metadata

	dtype := arrow.StructOf([]arrow.Field{
		{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
	}...)

	// New Struct Builder consists of :
	// 1. Data types
	// 2. Fields
	// 3  Builder
	// builder provides common functionality for managing the validity bitmap (nulls) when building arrays.

	sb := array.NewStructBuilder(pool, dtype)
	// Release is removing memory, with referece count to 0, mem freed.
	defer sb.Release()

	// field one builder
	// type cast to list builder
	f1b := sb.FieldBuilder(0).(*array.ListBuilder)
	defer f1b.Release()

	// field one value builder
	// type cast to Unsigned int8
	f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
	defer f1vb.Release()

	// field 2 builder, pass i as 1 to field,
	// type cast to int32 builder
	f2b := sb.FieldBuilder(1).(*array.Int32Builder)
	defer f2b.Release()

	// there are 4 structs array
	// [{‘joe’, 1}, {null, 2}, null, {‘mark’, 4}]
	// reserve 4
	sb.Reserve(4)

	// characters field one has total 7 characters
	// 1. Joe -- 3
	// 2. Null --
	// 3. Null --
	// 4. Mark -- 4
	// Total: 7
	f1vb.Reserve(7)

	// field 2 has total 3 int32
	// 1, 2 and 4
	f2b.Reserve(3)

	// Append to form {'joe', 1}
	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("joe"), nil)
	f2b.Append(1)

	// Append to form {'null', 2}
	sb.Append(true)
	f1b.AppendNull()
	f2b.Append(2)

	// Append to form null
	sb.AppendNull()

	// Append to form {'mark', 4}
	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("mark"), nil)
	f2b.Append(4)

	// new array
	arr := sb.NewArray().(*array.Struct)
	defer arr.Release()

	fmt.Printf("NullN() = %d\n", arr.NullN())
	fmt.Printf("Len()   = %d\n", arr.Len())

	// field 0 is a list
	list := arr.Field(0).(*array.List)
	defer list.Release()

	// [0 3 3 3 7]
	// joe has 3 chars + nul + nul + 4
	// 3 . 3 . 3 . 7 (add +4 )
	offsets := list.Offsets()
	fmt.Printf("%d\n", offsets)

	varr := list.ListValues().(*array.Uint8)
	defer varr.Release()

	ints := arr.Field(1).(*array.Int32)
	defer ints.Release()

	for i := 0; i < arr.Len(); i++ {
		if !arr.IsValid(i) {
			fmt.Printf("Struct[%d] = (null)\n", i)
			continue
		}
		fmt.Printf("Struct[%d] = [", i)
		pos := int(offsets[i])
		switch {
		case list.IsValid(pos):
			fmt.Printf("[")
			for j := offsets[i]; j < offsets[i+1]; j++ {
				if j != offsets[i] {
					fmt.Printf(", ")
				}
				fmt.Printf("%v", string(varr.Value(int(j))))
			}
			fmt.Printf("], ")
		default:
			fmt.Printf("(null), ")
		}
		fmt.Printf("%d]\n", ints.Value(i))
	}
}
