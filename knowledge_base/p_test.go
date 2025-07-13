package main

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestSchemaRoundTrip(t *testing.T) {
	// We create a schemas with all supported kinds, cardinalities, logical types, etc
	tests := []struct {
		name         string
		schema       *parquet.Schema
		roundTripped string
	}{
		{
			name: "bytes_strings_etc",
			schema: parquet.NewSchema("root", parquet.Group{
				"bytes_strings_etc": parquet.Optional(parquet.Group{
					"bson":         parquet.BSON(),
					"bytearray":    parquet.Leaf(parquet.ByteArrayType),
					"enum":         parquet.Enum(),
					"fixedbytes10": parquet.Leaf(parquet.FixedLenByteArrayType(10)),
					"fixedbytes20": parquet.Leaf(parquet.FixedLenByteArrayType(20)),
					"json":         parquet.JSON(),
					"string":       parquet.String(),
					"uuid":         parquet.UUID(),
				}),
			}),
			roundTripped: `message root {
	optional group bytes_strings_etc {
		required binary bson (BSON);
		required binary bytearray;
		required binary enum (ENUM);
		required fixed_len_byte_array(10) fixedbytes10;
		required fixed_len_byte_array(20) fixedbytes20;
		required binary json (JSON);
		required binary string (STRING);
		required fixed_len_byte_array(16) uuid (UUID);
	}
}`,
		},
		{
			name: "dates_times",
			schema: parquet.NewSchema("root", parquet.Group{
				"dates_times": parquet.Optional(parquet.Group{
					"date":                              parquet.Date(),
					"time_micros":                       parquet.Time(parquet.Microsecond),
					"time_millis":                       parquet.Time(parquet.Millisecond),
					"time_millis_not_utc_adjusted":      parquet.TimeAdjusted(parquet.Millisecond, false),
					"time_nanos":                        parquet.Time(parquet.Nanosecond),
					"timestamp_micros":                  parquet.Timestamp(parquet.Microsecond),
					"timestamp_millis":                  parquet.Timestamp(parquet.Millisecond),
					"timestamp_millis_not_utc_adjusted": parquet.TimestampAdjusted(parquet.Millisecond, false),
					"timestamp_nanos":                   parquet.Timestamp(parquet.Nanosecond),
				}),
			}),
			roundTripped: `message root {
	optional group dates_times {
		required int32 date (DATE);
		required int64 time_micros (TIME(isAdjustedToUTC=true,unit=MICROS));
		required int32 time_millis (TIME(isAdjustedToUTC=true,unit=MILLIS));
		required int32 time_millis_not_utc_adjusted (TIME(isAdjustedToUTC=false,unit=MILLIS));
		required int64 time_nanos (TIME(isAdjustedToUTC=true,unit=NANOS));
		required int64 timestamp_micros (TIMESTAMP(isAdjustedToUTC=true,unit=MICROS));
		required int64 timestamp_millis (TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS));
		required int64 timestamp_millis_not_utc_adjusted (TIMESTAMP(isAdjustedToUTC=false,unit=MILLIS));
		required int64 timestamp_nanos (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
	}
}`,
		},
		{
			name: "field_ids",
			schema: parquet.NewSchema("root", parquet.Group{
				"field_ids": parquet.Optional(parquet.Group{
					"f1":  parquet.FieldID(parquet.String(), 1),
					"f2":  parquet.FieldID(parquet.Int(32), 2),
					"f3":  parquet.FieldID(parquet.Uint(64), 3),
					"f4":  parquet.FieldID(parquet.Leaf(parquet.ByteArrayType), 4),
					"f_1": parquet.FieldID(parquet.String(), -1),
				}),
			}),
			roundTripped: `message root {
	optional group field_ids {
		required binary f1 (STRING) = 1;
		required int32 f2 (INT(32,true)) = 2;
		required int64 f3 (INT(64,false)) = 3;
		required binary f4 = 4;
		required binary f_1 (STRING) = -1;
	}
}`,
		},
		{
			name: "floats_and_decimals",
			schema: parquet.NewSchema("root", parquet.Group{
				"floats_and_decimals": parquet.Optional(parquet.Group{
					"decimal_fixed20": parquet.Decimal(2, 20, parquet.FixedLenByteArrayType(9)),
					"decimal_int15":   parquet.Decimal(1, 15, parquet.Int64Type),
					"decimal_int5":    parquet.Decimal(0, 5, parquet.Int32Type),
					// TODO: Decimal field backed by byte array.
					//       Spec allows it (https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal),
					//       but parquet-go currently panics:
					//			DECIMAL node must annotate Int32, Int64 or FixedLenByteArray but got BYTE_ARRAY
					//"decimal_bytes30": parquet.Decimal(3, 30, parquet.ByteArrayType),
					"double": parquet.Leaf(parquet.DoubleType),
					"float":  parquet.Leaf(parquet.FloatType),
				}),
			}),
			roundTripped: `message root {
	optional group floats_and_decimals {
		required fixed_len_byte_array(9) decimal_fixed20 (DECIMAL(20,2));
		required int64 decimal_int15 (DECIMAL(15,1));
		required int32 decimal_int5 (DECIMAL(5,0));
		required double double;
		required float float;
	}
}`,
		},
		{
			name: "ints",
			schema: parquet.NewSchema("root", parquet.Group{
				"ints": parquet.Optional(parquet.Group{
					"int16":  parquet.Int(16),
					"int32":  parquet.Leaf(parquet.Int32Type),
					"int32l": parquet.Int(32),
					"int64l": parquet.Int(64),
					"int64":  parquet.Leaf(parquet.Int64Type),
					"int8":   parquet.Int(8),
					"int96":  parquet.Leaf(parquet.Int96Type),
					"uint16": parquet.Uint(16),
					"uint32": parquet.Uint(32),
					"uint8":  parquet.Uint(8),
					"uint64": parquet.Uint(64),
				}),
			}),
			roundTripped: `message root {
	optional group ints {
		required int32 int16 (INT(16,true));
		required int32 int32 (INT(32,true));
		required int32 int32l (INT(32,true));
		required int64 int64 (INT(64,true));
		required int64 int64l (INT(64,true));
		required int32 int8 (INT(8,true));
		required int96 int96;
		required int32 uint16 (INT(16,false));
		required int32 uint32 (INT(32,false));
		required int64 uint64 (INT(64,false));
		required int32 uint8 (INT(8,false));
	}
}`,
		},
		{
			name: "lists",
			schema: parquet.NewSchema("root", parquet.Group{
				"lists": parquet.Optional(parquet.Group{
					"groups": parquet.List(parquet.Optional(parquet.Group{
						"a": parquet.String(),
						"b": parquet.Int(32),
					})),
					"ints":    parquet.List(parquet.Int(32)),
					"strings": parquet.Optional(parquet.List(parquet.String())),
				}),
			}),
			roundTripped: `message root {
	optional group lists {
		required group groups (LIST) {
			repeated group list {
				optional group element {
					required binary a (STRING);
					required int32 b (INT(32,true));
				}
			}
		}
		required group ints (LIST) {
			repeated group list {
				required int32 element (INT(32,true));
			}
		}
		optional group strings (LIST) {
			repeated group list {
				required binary element (STRING);
			}
		}
	}
}`,
		},
		{
			name: "maps",
			schema: parquet.NewSchema("root", parquet.Group{
				"maps": parquet.Optional(parquet.Group{
					"ints2ints":    parquet.Map(parquet.Int(32), parquet.Int(32)),
					"ints2strings": parquet.Optional(parquet.Map(parquet.Int(32), parquet.String())),
					"strings2groups": parquet.Map(parquet.String(), parquet.Optional(parquet.Group{
						"a": parquet.String(),
						"b": parquet.Int(32),
					})),
				}),
			}),
			roundTripped: `message root {
	optional group maps {
		required group ints2ints (MAP) {
			repeated group key_value {
				required int32 key (INT(32,true));
				required int32 value (INT(32,true));
			}
		}
		optional group ints2strings (MAP) {
			repeated group key_value {
				required int32 key (INT(32,true));
				required binary value (STRING);
			}
		}
		required group strings2groups (MAP) {
			repeated group key_value {
				required binary key (STRING);
				optional group value {
					required binary a (STRING);
					required int32 b (INT(32,true));
				}
			}
		}
	}
}`,
		},
		{
			name: "repeated_fields",
			schema: parquet.NewSchema("root", parquet.Group{
				"repeated_fields": parquet.Optional(parquet.Group{
					"groups": parquet.Repeated(parquet.Group{
						"a": parquet.String(),
						"b": parquet.Int(32),
					}),
					"ints":    parquet.Repeated(parquet.Int(32)),
					"strings": parquet.Repeated(parquet.String()),
				}),
			}),
			roundTripped: `message root {
	optional group repeated_fields {
		repeated group groups {
			required binary a (STRING);
			required int32 b (INT(32,true));
		}
		repeated int32 ints (INT(32,true));
		repeated binary strings (STRING);
	}
}`,
		},
		{
			name: "variants",
			schema: parquet.NewSchema("root", parquet.Group{
				"variants": parquet.Optional(parquet.Group{
					"unshredded": parquet.Optional(parquet.Variant()),
					// TODO: Also include shredded variants when they are implemented
				}),
			}),
			roundTripped: `message root {
	optional group variants {
		optional group unshredded (VARIANT) {
			required binary metadata;
			required binary value;
		}
	}
}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Write schema to file.
			var fileContents bytes.Buffer
			writer := parquet.NewGenericWriter[any](&fileContents, &parquet.WriterConfig{Schema: test.schema})
			// We need at least one row in the file, so synthesize a single row that is empty
			// with zero values for any required columns.
			row := make([]parquet.Value, len(test.schema.Columns()))
			for i, columnPath := range test.schema.Columns() {
				leaf, ok := test.schema.Lookup(columnPath...)
				if !ok {
					t.Fatalf("failed to look up path %q in schema", test.schema.Columns()[i])
				}
				if leaf.MaxDefinitionLevel > 0 {
					// optional
					row[i] = parquet.NullValue().Level(0, 0, i)
				} else {
					// required, so we need a value of the right type
					switch leaf.Node.Type().Kind() {
					case parquet.Boolean:
						row[i] = parquet.BooleanValue(false)
					case parquet.Int32:
						row[i] = parquet.Int32Value(0)
					case parquet.Int64:
						row[i] = parquet.Int64Value(0)
					case parquet.Int96:
						row[i] = parquet.Int96Value([3]uint32{0, 0, 0})
					case parquet.Float:
						row[i] = parquet.FloatValue(0)
					case parquet.Double:
						row[i] = parquet.DoubleValue(0)
					case parquet.ByteArray:
						row[i] = parquet.ByteArrayValue([]byte{})
					case parquet.FixedLenByteArray:
						row[i] = parquet.FixedLenByteArrayValue(make([]byte, leaf.Node.Type().Length()))
					}
				}
			}
			if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
				t.Fatalf("failed to write rows: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Read it back and make sure everything looks right.
			file, err := parquet.OpenFile(bytes.NewReader(fileContents.Bytes()), int64(fileContents.Len()))
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}
			if s := file.Schema().String(); s != test.roundTripped {
				t.Errorf("\nexpected:\n\n%s\n\nfound:\n\n%s\n", test.roundTripped, s)
			}
		})
	}
}
