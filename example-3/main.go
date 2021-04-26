package main

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func DetectType(data interface{}) string {

	d := reflect.ValueOf(data)
	switch d.Kind() {
	case reflect.Slice:
		return "Slice"
	case reflect.Map:
		return "Map"
	case reflect.String:
		return "String"
	case reflect.Float64:
		return "Float64"
	}
	return ""

}

func DetectArrowType(data interface{}) arrow.DataType {

	d := reflect.ValueOf(data)
	switch d.Kind() {
	case reflect.String:
		return arrow.BinaryTypes.String
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64
	}
	return nil

}

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

	jsonString := `[
		{
			"Name": "Adheip",
			"Age": 24
		}
	]`

	var data []interface{}

	json.Unmarshal([]byte(jsonString), &data)

	fields := make([]arrow.Field, len(data))

	for _, d := range data {
		if DetectType(d) == "Map" {
			for key, value := range d.(map[string]interface{}) {
				fields = append(fields, arrow.Field{
					Name:     key,
					Type:     DetectArrowType(value),
					Metadata: arrow.Metadata{},
				})
			}

		}
	}

	structFields := []arrow.Field{
		{
			Name: "Geek",
			Type: arrow.StructOf(fields[1:]...),
		},
	}
	fmt.Println(structFields)

	/*
		fields := []arrow.Field{
			{Name: "Geek", Type: arrow.StructOf([]arrow.Field{
				{
					Name: "Name", Type: arrow.BinaryTypes.String,
				},
				{
					Name: "Age", Type: arrow.PrimitiveTypes.Float64,
				},
			}...)},
		}
	*/
	schema := arrow.NewSchema(structFields, nil)

	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()

	sb := bld.Field(0).(*array.StructBuilder)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.StringBuilder)
	defer f1b.Release()

	f2b := sb.FieldBuilder(1).(*array.Float64Builder)
	defer f2b.Release()

	sb.AppendValues([]bool{true})
	f1b.AppendValues([]string{"Adheip"}, nil)
	f2b.AppendValues([]float64{24}, nil)

	rec1 := bld.NewRecord()
	defer rec1.Release()

	sb.AppendValues([]bool{true})
	f1b.AppendValues([]string{"Nitish"}, nil)
	f2b.AppendValues([]float64{32}, nil)

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
