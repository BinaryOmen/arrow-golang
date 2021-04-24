package main

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {

	pool := memory.NewGoAllocator()

	/*
		type Geek struct {
			Name string
			Age Float64
			Country struct {
				Code string
				City []string
			}
		}

		{
			"Name": "Adheip",
			"Age": 24
			"Country": {
				"Code": "IND",
				"City": [
					"Chandigarh",
					"Banglore",
				]
			}
		}
	*/
	fields := []arrow.Field{
		{Name: "Geek", Type: arrow.StructOf([]arrow.Field{
			{
				Name: "Name", Type: arrow.BinaryTypes.String,
			},
			{
				Name: "Age", Type: arrow.PrimitiveTypes.Float64,
			},
			{
				Name: "Country", Type: arrow.StructOf([]arrow.Field{
					{
						Name: "Code", Type: arrow.BinaryTypes.String,
					},
					{
						Name: "City", Type: arrow.ListOf(arrow.BinaryTypes.String),
					},
				}...),
			},
		}...)},
	}

	schema := arrow.NewSchema(fields, nil)

	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()

	sb := bld.Field(0).(*array.StructBuilder)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.StringBuilder)
	defer f1b.Release()

	f2b := sb.FieldBuilder(1).(*array.Float64Builder)
	defer f2b.Release()

	f3b := sb.FieldBuilder(2).(*array.StructBuilder)
	defer f3b.Release()

	f4lb := f3b.FieldBuilder(1).(*array.ListBuilder)
	defer f4lb.Release()

	f4b := f4lb.ValueBuilder().(*array.StringBuilder)
	defer f4b.Release()

	sb.AppendValues([]bool{true})
	f1b.AppendValues([]string{"Adheip"}, nil)
	f2b.AppendValues([]float64{24}, nil)

	f3b.AppendValues([]bool{true})
	f3b.FieldBuilder(0).(*array.StringBuilder).AppendValues([]string{"IND"}, nil)

	f4lb.Append(true)
	f4b.AppendValues([]string{"Chandigarh", "Banglore"}, nil)
	rec1 := bld.NewRecord()
	defer rec1.Release()

	sb.AppendValues([]bool{true})
	f1b.AppendValues([]string{"Nitish"}, nil)
	f2b.AppendValues([]float64{32}, nil)

	f3b.AppendValues([]bool{true})
	f3b.FieldBuilder(0).(*array.StringBuilder).AppendValues([]string{"IND"}, nil)
	f4lb.Append(true)
	f4b.AppendValues([]string{"Ranchi", "Banglore"}, nil)

	rec2 := bld.NewRecord()
	defer rec2.Release()

	tbl := array.NewTableFromRecords(schema, []array.Record{rec1, rec2})
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 5)
	defer tr.Release()

	n := 0
	for tr.Next() {
		rec := tr.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", n, rec.ColumnName(i), col)
		}
		n++
	}

}
