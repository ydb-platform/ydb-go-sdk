package main_test

import (
	"fmt"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"ydb-go-sdk/parser/yql/files"
)

type customErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (d *customErrorListener) SyntaxError(
	recognizer antlr.Recognizer,
	offendingSymbol interface{},
	line, column int,
	msg string,
	e antlr.RecognitionException,
) {
	d.errors = append(d.errors, fmt.Sprintf("Синтаксическая ошибка в %d:%d: %s", line, column, msg))
}

func parseInput(input string) []string {
	stream := antlr.NewInputStream(input)
	lexer := parsing.NewSQLv1Antlr4Lexer(stream)
	tokens := antlr.NewCommonTokenStream(lexer, 0)
	parser := parsing.NewSQLv1Antlr4Parser(tokens)

	// fmt.Println(tree.ToStringTree(nil, parser))

	errorListener := &customErrorListener{}
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)

	parser.Sql_query()

	return errorListener.errors
}

func TestParser(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "Test 1: SELECT * FROM table;",
			input:   "SELECT * FROM table;",
			wantErr: false,
		},
		{
			name:    "Test 2: SELECT * FROM",
			input:   "SELECT * FROM",
			wantErr: true,
		},
		{
			name:    "Test 3: Empty input",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := parseInput(tt.input)
			gotErr := len(errs) > 0
			if gotErr != tt.wantErr {
				t.Errorf("для ввода %q ожидали ошибок: %v, получили: %v. Ошибки: %v", tt.input, tt.wantErr, gotErr, errs)
			}
		})
	}
}
