package loader

import (
	"fmt"
	sparql "git.sr.ht/~gabe/hod/lang/ast"
	logpb "git.sr.ht/~gabe/hod/proto"
	//"reflect"
	"strings"
)

const (
	varRESOLVED   = "RESOLVED"
	varUNRESOLVED = ""
)

// struct to hold the graph of the query plan
type dependencyGraph struct {
	selectVars []string
	variables  map[string]bool
	terms      []*queryTerm
	plan       []queryTerm
}

func makeDependencyGraph(cursor *Cursor, vars []string, terms []*logpb.Triple) *dependencyGraph {
	dg := &dependencyGraph{
		selectVars: []string{},
		variables:  make(map[string]bool),
		terms:      make([]*queryTerm, len(terms)),
	}
	dg.selectVars = append(dg.selectVars, vars...)
	for i, term := range terms {
		dg.terms[i] = dg.makeQueryTerm(cursor, term)
	}

	// find term with fewest variables
	var next *queryTerm
rootLoop:
	for numvars := 1; numvars <= 3; numvars++ {
		for idx, term := range dg.terms {
			if len(term.variables) == numvars {
				next = term
				dg.terms = append(dg.terms[:idx], dg.terms[idx+1:]...)
				break rootLoop
			}
		}
	}
	if next != nil {
		dg.plan = append(dg.plan, *next)

		for len(dg.terms) > 0 {
			var idx int
			var term *queryTerm
			for idx, term = range dg.terms {
				if term.overlap(next) > 0 {
					break
				}
			}
			next = term
			dg.plan = append(dg.plan, *next)
			dg.terms = append(dg.terms[:idx], dg.terms[idx+1:]...)

		}
	}
	return dg
}

func (dg *dependencyGraph) dump() {
	for _, r := range dg.terms {
		r.dump(0)
		//fmt.Println(r)
	}
}

// stores the state/variables for a particular triple
// from a SPARQL query
type queryTerm struct {
	triple          *logpb.Triple
	subject, object EntityKey
	predicates      []edge
	dependencies    []*queryTerm
	variables       []string
}

// initializes a queryTerm from a given Filter
func (dg *dependencyGraph) makeQueryTerm(cursor *Cursor, t *logpb.Triple) *queryTerm {
	qt := &queryTerm{
		triple:       t,
		dependencies: []*queryTerm{},
		variables:    []string{},
	}
	qt.subject = cursor.ContextualizeURI(t.Subject)
	qt.object = cursor.ContextualizeURI(t.Object)
	for _, pred := range t.Predicate {
		qt.predicates = append(qt.predicates, edge{predicate: cursor.ContextualizeURI(pred), pattern: pred.Pattern})
	}

	if isVariable(t.Subject) {
		dg.variables[t.Subject.Value] = false
		qt.variables = append(qt.variables, t.Subject.Value)
	}
	if isVariable(t.Predicate[0]) {
		dg.variables[t.Predicate[0].Value] = false
		qt.variables = append(qt.variables, t.Predicate[0].Value)
	}
	if isVariable(t.Object) {
		dg.variables[t.Object.Value] = false
		qt.variables = append(qt.variables, t.Object.Value)
	}
	return qt
}

func makeQueryTerm(cursor *Cursor, t *logpb.Triple) *queryTerm {
	qt := &queryTerm{
		triple:       t,
		dependencies: []*queryTerm{},
		variables:    []string{},
	}
	qt.subject = cursor.ContextualizeURI(t.Subject)
	qt.object = cursor.ContextualizeURI(t.Object)
	for _, pred := range t.Predicate {
		qt.predicates = append(qt.predicates, edge{predicate: cursor.ContextualizeURI(pred), pattern: pred.Pattern})
	}

	if isVariable(t.Subject) {
		qt.variables = append(qt.variables, t.Subject.Value)
	}
	if isVariable(t.Predicate[0]) {
		qt.variables = append(qt.variables, t.Predicate[0].Value)
	}
	if isVariable(t.Object) {
		qt.variables = append(qt.variables, t.Object.Value)
	}
	return qt
}

func (qt *queryTerm) String() string {
	return fmt.Sprintf("<%v %v %v>", qt.subject, qt.predicates[0], qt.object)
}

func (qt *queryTerm) dump(indent int) {
	fmt.Println(strings.Repeat("  ", indent), qt.String())
	for _, c := range qt.dependencies {
		c.dump(indent + 1)
	}
}

func (qt *queryTerm) overlap(other *queryTerm) int {
	count := 0
	for _, v := range qt.variables {
		for _, vv := range other.variables {
			if vv == v {
				count++
			}
		}
	}
	return count
}

// need operator types that go into the query plan
// Types:
//  SELECT: given a 2/3 triple, it resolves the 3rd item
//  FILTER: given a 1/3 triple, it restricts the other 2 items

// the old "queryplan" file is really a dependency graph for the query: it is NOT
// the queryplanner. What we should do now is take that dependency graph and turn
// it into a query plan

func formQueryPlan(dg *dependencyGraph, q *sparql.Query) (*queryPlan, error) {
	plan := newQueryPlan(dg, q)

	for _, term := range dg.plan {
		var (
			subjectIsVariable = isVariable(term.triple.Subject)
			objectIsVariable  = isVariable(term.triple.Object)
			// for now just look at first item in path
			predicateIsVariable  = isVariable(term.triple.Predicate[0])
			subjectVar           = term.triple.Subject.Value
			objectVar            = term.triple.Object.Value
			predicateVar         = term.triple.Predicate[0].Value
			hasResolvedSubject   bool
			hasResolvedObject    bool
			hasResolvedPredicate bool
			newop                operation
			numvars              = len(term.variables)
		)
		hasResolvedSubject = plan.hasVar(subjectVar)
		hasResolvedObject = plan.hasVar(objectVar)
		hasResolvedPredicate = plan.hasVar(predicateVar)

		switch {
		// definitions: do these first
		case numvars == 1 && subjectIsVariable:
			newop = &resolveSubject{term: term}
			if !plan.varIsChild(subjectVar) {
				plan.addTopLevel(subjectVar)
			}
		case numvars == 1 && objectIsVariable:
			// s p ?o
			newop = &resolveObject{term: term}
			if !plan.varIsChild(objectVar) {
				plan.addTopLevel(objectVar)
			}
		case numvars == 1 && predicateIsVariable:
			// s ?p o
			newop = &resolvePredicate{term: term}
			if !plan.varIsChild(predicateVar) {
				plan.addTopLevel(predicateVar)
			}
		// terms with 3 variables
		case subjectIsVariable && objectIsVariable && predicateIsVariable:
			switch {
			case hasResolvedSubject:
				newop = &resolveVarTripleFromSubject{term: term}
			case hasResolvedObject:
				newop = &resolveVarTripleFromObject{term: term}
			case hasResolvedPredicate:
				newop = &resolveVarTripleFromPredicate{term: term}
			default: // all are vars
				newop = &resolveVarTripleAll{term: term}
			}
		// subject/object variable terms
		case subjectIsVariable && objectIsVariable && !predicateIsVariable:
			switch {
			case hasResolvedSubject && hasResolvedObject:
				// if we have both subject and object, we filter
				rso := &restrictSubjectObjectByPredicate{term: term}
				if plan.varIsChild(subjectVar) {
					plan.addLink(subjectVar, objectVar)
				} else if plan.varIsChild(objectVar) {
					plan.addLink(objectVar, subjectVar)
				} else if plan.varIsTop(subjectVar) {
					plan.addLink(subjectVar, objectVar)
				} else if plan.varIsTop(objectVar) {
					plan.addLink(objectVar, subjectVar)
				}
				newop = rso
			case hasResolvedObject:
				newop = &resolveSubjectFromVarObject{term: term}
				plan.addLink(objectVar, subjectVar)
			case hasResolvedSubject:
				newop = &resolveObjectFromVarSubject{term: term}
				plan.addLink(subjectVar, objectVar)
			default:
				//newop = &resolveSubjectObjectFromPred{term: term}
				newop = &restrictSubjectObjectByPredicate{term: term}
				plan.addTopLevel(subjectVar)
				plan.addLink(subjectVar, objectVar)
			}
		case !subjectIsVariable && !objectIsVariable && predicateIsVariable:
			newop = &resolvePredicate{term: term}
			if !plan.varIsChild(predicateVar) {
				plan.addTopLevel(predicateVar)
			}
		case subjectIsVariable && !objectIsVariable && predicateIsVariable:
			// ?s ?p o
			newop = &resolveSubjectPredFromObject{term: term}
			plan.addLink(subjectVar, predicateVar)
		case !subjectIsVariable && objectIsVariable && predicateIsVariable:
			// s ?p ?o
			newop = &resolvePredObjectFromSubject{term: term}
			plan.addLink(objectVar, predicateVar)
		case subjectIsVariable:
			// ?s p o
			newop = &resolveSubject{term: term}
			if !plan.varIsChild(subjectVar) {
				plan.addTopLevel(subjectVar)
			}
		case objectIsVariable:
			// s p ?o
			newop = &resolveObject{term: term}
			if !plan.varIsChild(objectVar) {
				plan.addTopLevel(objectVar)
			}
		default:
			return plan, fmt.Errorf("Nothing chosen for %v. This shouldn't happen", term)
		}
		plan.operations = append(plan.operations, newop)
	}
	return plan, nil
}

// contains all useful state information for executing a query
type queryPlan struct {
	operations []operation
	selectVars []string
	dg         *dependencyGraph
	query      *sparql.Query
	vars       map[string]string
	variables  []string
}

func newQueryPlan(dg *dependencyGraph, q *sparql.Query) *queryPlan {
	plan := &queryPlan{
		selectVars: dg.selectVars,
		dg:         dg,
		query:      q,
		vars:       make(map[string]string),
	}
	if q != nil {
		plan.variables = q.Variables
	}
	return plan
}

//func (plan *queryPlan) dumpVarchain() {
//	for k, v := range plan.vars {
//		fmt.Println(k, "=>", v)
//	}
//}

func (plan *queryPlan) hasVar(variable string) bool {
	return plan.vars[variable] != varUNRESOLVED
}

func (plan *queryPlan) varIsChild(variable string) bool {
	return plan.hasVar(variable) && plan.vars[variable] != varRESOLVED
}

func (plan *queryPlan) varIsTop(variable string) bool {
	return plan.hasVar(variable) && plan.vars[variable] == varRESOLVED
}

func (plan *queryPlan) addTopLevel(variable string) {
	plan.vars[variable] = varRESOLVED
}

func (plan *queryPlan) addLink(parent, child string) {
	plan.vars[child] = parent
}

func isVariable(uri *logpb.URI) bool {
	return uri == nil || strings.HasPrefix(uri.Value, "?")
}
