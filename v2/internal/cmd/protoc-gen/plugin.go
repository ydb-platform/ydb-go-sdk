package main

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/generator"
)

func init() {
	generator.RegisterPlugin(new(plugin))
}

type plugin struct {
	*generator.Generator
}

func (p *plugin) Name() string {
	return "ydb"
}

func (p *plugin) Init(g *generator.Generator) {
	p.Generator = g
}

func (p *plugin) Generate(file *generator.FileDescriptor) {
	p.P("const (")
	for _, service := range file.FileDescriptorProto.Service {
		for _, method := range service.Method {
			sname := service.GetName()
			if pkg := file.GetPackage(); pkg != "" {
				sname = pkg + "." + sname
			}
			mname := method.GetName()

			p.P(fmt.Sprintf(
				`%[2]s = "/%[1]s/%[2]s"`,
				sname, mname,
			))
		}
	}
	p.P(")")

	const (
		fieldName = "operation_params"
		fieldType = ".Ydb.Operations.OperationParams"
	)
	for _, message := range file.FileDescriptorProto.MessageType {
		if message.Name == nil {
			continue
		}
		messageName := *(message.Name)

		for _, field := range message.Field {
			if field.Name == nil || field.TypeName == nil {
				continue
			}
			if *(field.Name) != fieldName {
				continue
			}
			if *(field.TypeName) != fieldType {
				continue
			}

			fieldName := goFieldName(*(field.Name))

			pkg, typ := goFieldType(*(field.TypeName))
			fieldType := typ
			if pkg != "" {
				fieldType = pkg + "." + fieldType
			}
			fieldType = "*" + fieldType

			p.P(fmt.Sprintf(
				`
				// Set%[2]s implements ydb generic interface for setting
				// operation parameters inside driver implementation.
				func (m *%[1]s) Set%[2]s(v %[3]s) {
					m.%[2]s = v
				}
				`,
				messageName,
				fieldName,
				fieldType,
			))
		}
	}
}

func (p *plugin) GenerateImports(file *generator.FileDescriptor) {}

func goFieldName(s string) string {
	var hs []string
	for _, h := range strings.Split(s, "_") {
		hs = append(hs, strings.Title(h))
	}
	return strings.Join(hs, "")
}

func goFieldType(s string) (pkg, typ string) {
	hs := strings.Split(strings.TrimPrefix(s, "."), ".")
	last := len(hs) - 1
	return strings.Join(hs[:last], "_"), hs[last]
}
