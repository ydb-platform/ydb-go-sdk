package parser

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/parser/yql"
)

type treeShapeListener struct {
	*yql.BaseYQLListener

	data []string
}

func (l *treeShapeListener) EnterEveryRule(ctx antlr.ParserRuleContext) {
	l.data = append(l.data, ctx.GetText())
}

func TestParserYQL(t *testing.T) {
	input := antlr.NewInputStream(`SELECT 1`)
	lexer := yql.NewYQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)
	parser := yql.NewYQLParser(stream)
	parser.AddErrorListener(antlr.NewDiagnosticErrorListener(true))
	stmt := parser.Select_stmt()
	listener := &treeShapeListener{}
	antlr.NewParseTreeWalker().Walk(listener, stmt)
	require.Equal(t,
		[]string{
			"SELECT1",
			"SELECT1",
			"SELECT1",
			"SELECT1",
			"SELECT1",
			"",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"1",
			"",
		},
		listener.data,
	)
}
