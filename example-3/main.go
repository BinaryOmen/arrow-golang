package main

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/arrow"
)

func DetectType(data interface{}) string {

	d := reflect.ValueOf(data)
	switch d.Kind() {
	case reflect.Slice:
		return "Slice"
	case reflect.Map:
		return "Map"
	case reflect.Struct:
		return "Struct"
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
	case reflect.Map:
		return arrow.StructOf(jsonToArrow(data, nil)...)
	}
	return nil
}

func jsonToArrow(data interface{}, fields []arrow.Field) []arrow.Field {

	if DetectType(data) == "Map" {
		for key, value := range data.(map[string]interface{}) {
			fields = append(fields, arrow.Field{
				Name:     key,
				Type:     DetectArrowType(value),
				Metadata: arrow.Metadata{},
			})
		}
	}

	return fields
}

func createArrowFields(data []interface{}) []arrow.Field {
	fields := make([]arrow.Field, len(data))
	for _, d := range data {
		fields = jsonToArrow(d, fields)
		return fields
	}
	return nil
}

func main() {

	//pool := memory.NewGoAllocator()

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

	jsonString := `
	[
	{
		"Name": "Adheip",
		"Age": 24,
		"Country": {
			"Code": 24
		},
		"City": {
			"Punjab": "India"
		}
	}
	]`

	var data []interface{}

	json.Unmarshal([]byte(jsonString), &data)
	//fmt.Println(createArrowFields(data)[1:])

	structFields := []arrow.Field{
		{
			Name: "Geek",
			Type: arrow.StructOf(createArrowFields(data)[1:]...),
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
	*/

}
