//go:generate gocc -p git.sr.ht/~gabe/hod/lang -a sparql.bnf
package sparql

import (
	"git.sr.ht/~gabe/hod/lang/ast"
	"git.sr.ht/~gabe/hod/lang/lexer"
	"git.sr.ht/~gabe/hod/lang/parser"
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
