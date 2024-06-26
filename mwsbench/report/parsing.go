package report

import (
	"fmt"
	"reflect"

	"github.com/lesismal/perf"
)

func Init(enableTPN bool) {
	appendTPNHeadder := func(headers []string, field reflect.StructField) []string {
		header := field.Tag.Get("md")
		if header != "-" {
			if enableTPN {
				headers = append(headers, header)
				return headers
			}
			isTPN := field.Tag.Get("tpn") != ""
			if !isTPN {
				headers = append(headers, header)
			}
		}
		return headers
	}
	typ := reflect.TypeOf(ConnectionsReport{})
	for i := 0; i < typ.NumField(); i++ {
		ConnectionsReportMarkdownHeaders = appendTPNHeadder(ConnectionsReportMarkdownHeaders, typ.Field(i))
	}

	typ = reflect.TypeOf(BenchEchoReport{})
	for i := 0; i < typ.NumField(); i++ {
		BenchEchoReportMarkdownHeaders = appendTPNHeadder(BenchEchoReportMarkdownHeaders, typ.Field(i))
	}

	typ = reflect.TypeOf(BenchRateReport{})
	for i := 0; i < typ.NumField(); i++ {
		header := typ.Field(i).Tag.Get("md")
		if header != "-" {
			BenchRateReportMarkdownHeaders = append(BenchRateReportMarkdownHeaders, header)
		}
	}
}

func ObjFieldValues(obj interface{}, enableTPN bool) []string {
	values := []string{}
	typ := reflect.TypeOf(obj)
	value := reflect.ValueOf(obj)
	if typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct {
		typ = typ.Elem()
		value = value.Elem()
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldValue := value.FieldByName(field.Name)

		isTPN := field.Tag.Get("tpn") != ""
		if !enableTPN && isTPN {
			continue
		}

		switch field.Tag.Get("fmt") {
		case "mem":
			if fieldValue.CanInt() {
				values = append(values, perf.I2MemString(uint64(fieldValue.Int())))
			} else if fieldValue.CanUint() {
				values = append(values, perf.I2MemString(uint64(fieldValue.Uint())))
			} else {
				values = append(values, "")
			}
		case "duration":
			values = append(values, perf.I2TimeString(fieldValue.Int()))
		case "-":
		default:
			typName := field.Type.Name()
			switch typName {
			case "string":
				values = append(values, fieldValue.String())
			case "float32", "float64":
				values = append(values, fmt.Sprintf("%.2f", fieldValue.Float()))
			default:
				values = append(values, fmt.Sprintf("%v", fieldValue))
			}
		}
	}
	return values
}

func ObjString(obj Report, enableTPN bool) string {
	ret := ""
	headers := []string{}
	values := []string{}
	typ := reflect.TypeOf(obj)
	value := reflect.ValueOf(obj)
	if typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct {
		typ = typ.Elem()
		value = value.Elem()
	}
	typHeader := "BenchType"
	maxHeaderLen := len(typHeader)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		header := field.Tag.Get("md")
		if header == "-" {
			continue
		}
		if !enableTPN {
			isTPN := field.Tag.Get("tpn") != ""
			if isTPN {
				continue
			}
		}
		headers = append(headers, header)
		if len(header) > maxHeaderLen {
			maxHeaderLen = len(header)
		}

		fieldValue := value.FieldByName(field.Name)
		switch field.Tag.Get("fmt") {
		case "cpu":
			values = append(values, fmt.Sprintf("%.2f%%", fieldValue.Float()))
		case "mem":
			if fieldValue.CanInt() {
				values = append(values, perf.I2MemString(uint64(fieldValue.Int())))
			} else if fieldValue.CanUint() {
				values = append(values, perf.I2MemString(uint64(fieldValue.Uint())))
			} else {
				values = append(values, "")
			}
		case "duration":
			values = append(values, perf.I2TimeString(fieldValue.Int()))
		case "-":
		default:
			typName := field.Type.Name()
			switch typName {
			case "string":
				values = append(values, fieldValue.String())
			case "float32", "float64":
				values = append(values, fmt.Sprintf("%.2f", fieldValue.Float()))
			default:
				values = append(values, fmt.Sprintf("%v", fieldValue))
			}
		}
	}
	frameworkHeader := ""
	for i, v := range headers {
		isFramework := v == "Framework"
		for len(v) < maxHeaderLen {
			v += " "
		}
		if isFramework {
			frameworkHeader = v
		}
		headers[i] = v
	}
	for len(typHeader) < maxHeaderLen {
		typHeader += " "
	}
	for i, v := range headers {
		if v == frameworkHeader {
			ret += typHeader + ": " + obj.Type() + "\n"
		}
		ret += v + ": " + values[i]
		if i != len(headers)-1 {
			ret += "\n"
		}
	}
	return ret
}
