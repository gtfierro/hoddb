//go:generate gocc -p github.com/gtfierro/hoddb/lang -a sparql.bnf
package sparql

import (
	"github.com/gtfierro/hoddb/lang/ast"
	"github.com/gtfierro/hoddb/lang/lexer"
	"github.com/gtfierro/hoddb/lang/parser"
	"sync"
)

var p *parser.Parser
var l sync.Mutex

func init() {
	p = parser.NewParser()
}

func Parse(s string) (*ast.Query, error) {
	l.Lock()
	defer l.Unlock()
	lexed := lexer.NewLexer([]byte(s))
	_q, err := p.Parse(lexed)
	if err != nil {
		return nil, err
	}
	q := _q.(ast.Query)
	return &q, nil
}
