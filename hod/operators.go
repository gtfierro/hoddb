package hod

import (
	"fmt"
	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("Key Not Found")

type operation interface {
	run(cursor *Cursor) error
	String() string
	GetTerm() queryTerm
}

// ?subject predicate object
// Find all subjects part of triples with the given predicate and object
type resolveSubject struct {
	term queryTerm
}

func (rs *resolveSubject) String() string {
	return fmt.Sprintf("[resolveSubject %s]", rs.term.triple)
}

func (rs *resolveSubject) GetTerm() queryTerm {
	return rs.term
}

func (rs *resolveSubject) run(cursor *Cursor) error {
	// fetch the object from the graph

	object, err := cursor.getEntity(rs.term.object)
	if err != nil && err != ErrNotFound {
		return errors.Wrap(err, fmt.Sprintf("%+v", rs.term.triple))
	} else if err == ErrNotFound {
		return nil
	}
	subjectVar := rs.term.triple.Subject.Value
	// get all subjects reachable from the given object along the path
	subjects, err := cursor.getSubjectFromPredObject(object, rs.term.predicates)
	if err != nil {
		return err
	}

	cursor.addOrJoin(subjectVar, subjects)

	return nil
}

// object predicate ?object
// Find all objects part of triples with the given predicate and subject
type resolveObject struct {
	term queryTerm
}

func (ro *resolveObject) String() string {
	return fmt.Sprintf("[resolveObject %s]", ro.term.triple)
}

func (ro *resolveObject) GetTerm() queryTerm {
	return ro.term
}

func (ro *resolveObject) run(cursor *Cursor) error {
	// fetch the subject from the graph
	subject, err := cursor.getEntity(ro.term.subject)
	if err != nil && err != ErrNotFound {
		return errors.Wrap(err, fmt.Sprintf("%+v", ro.term.triple))
	} else if err == ErrNotFound {
		return nil
	}
	objectVar := ro.term.triple.Object.Value
	objects, err := cursor.getObjectFromSubjectPred(subject, ro.term.predicates)
	if err != nil {
		return err
	}

	cursor.addOrJoin(objectVar, objects)
	return nil
}

// object ?predicate object
// Find all predicates part of triples with the given subject and subject
type resolvePredicate struct {
	term queryTerm
}

func (op *resolvePredicate) String() string {
	return fmt.Sprintf("[resolvePredicate %s]", op.term.triple)
}

func (op *resolvePredicate) GetTerm() queryTerm {
	return op.term
}

func (op *resolvePredicate) run(cursor *Cursor) error {
	predicateVar := op.term.triple.Predicate[0].Value
	// get all preds w/ the given end object, starting from the given subject

	predicates, err := cursor.getPredicateFromSubjectObject(op.term.subject, op.term.object)
	if err != nil {
		return err
	}

	cursor.addOrJoin(predicateVar, predicates)
	return nil
}

// ?sub pred ?obj
// Find all subjects and objects that have the given relationship
type restrictSubjectObjectByPredicate struct {
	term queryTerm
}

func (rso *restrictSubjectObjectByPredicate) String() string {
	return fmt.Sprintf("[restrictSubObjByPred %s]", rso.term.triple)
}

func (rso *restrictSubjectObjectByPredicate) GetTerm() queryTerm {
	return rso.term
}

// there are 4 cases, depending on the structure/content of the existing relation in the cursor
// cursor: (# of values)
//          +--------------+---------------+
//          | ?subject = 0 |  ?object = 0  |
//          | ?subject > 0 |  ?object = 0  |
//          | ?subject = 0 |  ?object > 0  |
//          | ?subject > 0 |  ?object > 0  |
//          +--------------+---------------+
//  For the given predicate we look up the subjects and objects that exist. This creates a relation.
//  In the first case, we replace the cursor columns with the values in the new relation.
//  In the second and third cases, we join on the variable that has values (?subject in the first, ?object
//  in the second).
//  In the fourth case, we need to join on both variables.
func (rso *restrictSubjectObjectByPredicate) run(cursor *Cursor) error {
	var (
		subjectVar = rso.term.triple.Subject.Value
		objectVar  = rso.term.triple.Object.Value
	)

	// this operator takes existing values for subjects and objects and finds the pairs of them that
	// are connected by the path defined by rso.term.Predicates.

	var rsopRelation = newRelation([]string{subjectVar, objectVar})

	// use whichever variable has already been joined on, which means
	// that there are values in the relation that we can join with

	// TODO: for the whole path
	pairs, err := cursor.getSubjectObjectFromPred(rso.term.predicates[0])
	if err != nil {
		return err
	}
	rsopRelation.add2Values(subjectVar, objectVar, pairs)

	hasSubjectValues := cursor.hasValuesFor(subjectVar)
	hasObjectValues := cursor.hasValuesFor(objectVar)

	if hasSubjectValues && hasObjectValues {
		cursor.rel.join(rsopRelation, []string{subjectVar, objectVar}, cursor)
	} else if hasSubjectValues && !hasObjectValues {
		cursor.rel.join(rsopRelation, []string{subjectVar}, cursor)
	} else if !hasSubjectValues && hasObjectValues {
		cursor.rel.join(rsopRelation, []string{objectVar}, cursor)
	} else {
		// TODO: need to merge in WITHOUT replacing other values
		cursor.rel.rows = rsopRelation.rows
	}
	return nil
}

// ?sub pred ?obj, but we have already resolved the object
// For each of the current
type resolveSubjectFromVarObject struct {
	term queryTerm
}

func (rsv *resolveSubjectFromVarObject) String() string {
	return fmt.Sprintf("[resolveSubFromVarObj %s]", rsv.term.triple)
}

func (rsv *resolveSubjectFromVarObject) GetTerm() queryTerm {
	return rsv.term
}

// Use this when we have subject and object variables, but only object has been filled in.
//
func (rsv *resolveSubjectFromVarObject) run(cursor *Cursor) error {
	var (
		objectVar  = rsv.term.triple.Object.Value
		subjectVar = rsv.term.triple.Subject.Value
	)

	var rsopRelation = newRelation([]string{objectVar, subjectVar})
	var relationContents [][]EntityKey

	objects := cursor.getValuesFor(objectVar)
	for objectKey := range objects {
		object, err := cursor.getEntity(objectKey)
		if err != nil {
			return errors.Wrapf(err, "getObjectFromHash %s", objectKey)
		}
		reachableSubjects, err := cursor.getSubjectFromPredObject(object, rsv.term.predicates)
		if err != nil {
			return errors.Wrap(err, "getSubjectFromPredObject")
		}
		for subject := range reachableSubjects {
			relationContents = append(relationContents, []EntityKey{objectKey, subject})
		}
	}

	rsopRelation.add2Values(objectVar, subjectVar, relationContents)
	cursor.rel.join(rsopRelation, []string{objectVar}, cursor)

	return nil
}

type resolveObjectFromVarSubject struct {
	term queryTerm
}

func (rov *resolveObjectFromVarSubject) String() string {
	return fmt.Sprintf("[resolveObjFromVarSub %s]", rov.term.triple)
}

func (rov *resolveObjectFromVarSubject) GetTerm() queryTerm {
	return rov.term
}

func (rov *resolveObjectFromVarSubject) run(cursor *Cursor) error {
	var (
		objectVar  = rov.term.triple.Object.Value
		subjectVar = rov.term.triple.Subject.Value
	)

	var rsopRelation = newRelation([]string{subjectVar, objectVar})
	var relationContents [][]EntityKey

	subjects := cursor.getValuesFor(subjectVar)
	for subjectKey := range subjects {
		subject, err := cursor.getEntity(subjectKey)
		if err != nil {
			return errors.Wrapf(err, "get key %v", subjectKey)
		}
		reachableObjects, err := cursor.getObjectFromSubjectPred(subject, rov.term.predicates)
		if err != nil {
			return err
		}
		for object := range reachableObjects {
			relationContents = append(relationContents, []EntityKey{subjectKey, object})
		}
	}

	rsopRelation.add2Values(subjectVar, objectVar, relationContents)
	cursor.rel.join(rsopRelation, []string{subjectVar}, cursor)

	return nil
}

type resolveObjectFromVarSubjectPred struct {
	term queryTerm
}

func (op *resolveObjectFromVarSubjectPred) String() string {
	return fmt.Sprintf("[resolveObjFromVarSubPred %s]", op.term.triple)
}

func (op *resolveObjectFromVarSubjectPred) GetTerm() queryTerm {
	return op.term
}

// ?s ?p o
func (op *resolveObjectFromVarSubjectPred) run(cursor *Cursor) error {
	return nil
}

type resolveSubjectPredFromObject struct {
	term queryTerm
}

func (op *resolveSubjectPredFromObject) String() string {
	return fmt.Sprintf("[resolveSubPredFromObj %s]", op.term.triple)
}

func (op *resolveSubjectPredFromObject) GetTerm() queryTerm {
	return op.term
}

// we have an object and want to find subjects/predicates that connect to it.
// If we have partially resolved the predicate, then we iterate through those connected to
// the known object and then pull the associated subjects. We then filter those subjects
// by anything we've already resolved.
// If we have *not* resolved the predicate, then this is easy: just graph traverse from the object
func (op *resolveSubjectPredFromObject) run(cursor *Cursor) error {
	subjectVar := op.term.triple.Subject.Value
	predicateVar := op.term.triple.Predicate[0].Value

	// fetch the object from the graph
	object, err := cursor.getEntity(op.term.object)
	if err != nil && err != ErrNotFound {
		return errors.Wrap(err, fmt.Sprintf("%+v", op.term.triple))
	} else if err == ErrNotFound {
		return nil
	}

	// create a relation from all of the edges from that object
	objectrelation := newRelation([]string{subjectVar, predicateVar})
	objectrelation.add2Values(predicateVar, subjectVar, object.GetAllInEdges())

	hasSubjectValues := cursor.hasValuesFor(subjectVar)
	hasPredValues := cursor.hasValuesFor(predicateVar)
	if hasSubjectValues && hasPredValues {
		cursor.rel.join(objectrelation, []string{subjectVar, predicateVar}, cursor)
	} else if hasSubjectValues && !hasPredValues {
		cursor.rel.join(objectrelation, []string{subjectVar}, cursor)
	} else if !hasSubjectValues && hasPredValues {
		cursor.rel.join(objectrelation, []string{predicateVar}, cursor)
	} else {
		cursor.rel.rows = objectrelation.rows
	}

	return nil
}

type resolvePredObjectFromSubject struct {
	term queryTerm
}

func (op *resolvePredObjectFromSubject) String() string {
	return fmt.Sprintf("[resolvePredObjectFromSubject %s]", op.term.triple)
}

func (op *resolvePredObjectFromSubject) GetTerm() queryTerm {
	return op.term
}

func (op *resolvePredObjectFromSubject) run(cursor *Cursor) error {
	objectVar := op.term.triple.Object.Value
	predicateVar := op.term.triple.Predicate[0].Value

	// fetch the subject from the graph
	subject, err := cursor.getEntity(op.term.subject)
	if err != nil && err != ErrNotFound {
		return errors.Wrap(err, fmt.Sprintf("%+v", op.term.triple))
	} else if err == ErrNotFound {
		return nil
	}

	// create a relation from all of the edges from that object
	objectrelation := newRelation([]string{predicateVar, objectVar})
	objectrelation.add2Values(predicateVar, objectVar, subject.GetAllOutEdges())

	hasObjectValues := cursor.hasValuesFor(objectVar)
	hasPredValues := cursor.hasValuesFor(predicateVar)
	if hasObjectValues && hasPredValues {
		cursor.rel.join(objectrelation, []string{predicateVar, objectVar}, cursor)
	} else if hasObjectValues && !hasPredValues {
		cursor.rel.join(objectrelation, []string{objectVar}, cursor)
	} else if !hasObjectValues && hasPredValues {
		cursor.rel.join(objectrelation, []string{predicateVar}, cursor)
	} else {
		cursor.rel.rows = objectrelation.rows
	}

	return nil
}

type resolveVarTripleFromSubject struct {
	term queryTerm
}

func (op *resolveVarTripleFromSubject) String() string {
	return fmt.Sprintf("[resolveVarTripleFromSubject %s]", op.term.triple)
}

func (op *resolveVarTripleFromSubject) GetTerm() queryTerm {
	return op.term
}

// ?s ?p ?o; start from s
func (op *resolveVarTripleFromSubject) run(cursor *Cursor) error {
	//// for all subjects, find all predicates and objects. Note: these predicates
	//// and objects may be partially evaluated already
	var (
		subjectVar   = op.term.triple.Subject.Value
		objectVar    = op.term.triple.Object.Value
		predicateVar = op.term.triple.Predicate[0].Value
	)

	var rsopRelation = newRelation([]string{subjectVar, predicateVar, objectVar})
	var relationContents [][]EntityKey

	subjects := cursor.getValuesFor(subjectVar)
	for subjectKey := range subjects {
		subject, err := cursor.getEntity(subjectKey)
		if err != nil {
			return err
		}
		for _, edge := range subject.GetAllOutEdges() {
			relationContents = append(relationContents, []EntityKey{subjectKey, edge[0], edge[1]})
		}
	}
	rsopRelation.add3Values(subjectVar, predicateVar, objectVar, relationContents)
	cursor.join(rsopRelation, []string{subjectVar})

	return nil
}

type resolveVarTripleFromObject struct {
	term queryTerm
}

func (op *resolveVarTripleFromObject) String() string {
	return fmt.Sprintf("[resolveVarTripleFromObject %s]", op.term.triple)
}

func (op *resolveVarTripleFromObject) GetTerm() queryTerm {
	return op.term
}

// ?s ?p ?o; start from o
func (op *resolveVarTripleFromObject) run(cursor *Cursor) error {
	var (
		subjectVar   = op.term.triple.Subject.Value
		objectVar    = op.term.triple.Object.Value
		predicateVar = op.term.triple.Predicate[0].Value
	)

	var rsopRelation = newRelation([]string{subjectVar, predicateVar, objectVar})
	var relationContents [][]EntityKey

	objects := cursor.getValuesFor(objectVar)
	for objectKey := range objects {
		object, err := cursor.getEntity(objectKey)
		if err != nil {
			return err
		}
		//cursor.L.Dump(object)
		for _, edge := range object.GetAllInEdges() {
			relationContents = append(relationContents, []EntityKey{edge[0], edge[1], objectKey})
		}
	}
	rsopRelation.add3Values(subjectVar, predicateVar, objectVar, relationContents)
	cursor.join(rsopRelation, []string{objectVar})
	return nil
}

type resolveVarTripleFromPredicate struct {
	term queryTerm
}

func (op *resolveVarTripleFromPredicate) String() string {
	return fmt.Sprintf("[resolveVarTripleFromPredicate %s]", op.term.triple)
}

func (op *resolveVarTripleFromPredicate) GetTerm() queryTerm {
	return op.term
}

// ?s ?p ?o; start from p
func (op *resolveVarTripleFromPredicate) run(cursor *Cursor) error {
	var (
		subjectVar   = op.term.triple.Subject.Value
		objectVar    = op.term.triple.Object.Value
		predicateVar = op.term.triple.Predicate[0].Value
	)

	var rsopRelation = newRelation([]string{subjectVar, predicateVar, objectVar})
	var relationContents [][]EntityKey

	predicates := cursor.getValuesFor(predicateVar)
	for predicateKey := range predicates {
		predicate, err := cursor.getEntity(predicateKey)
		if err != nil {
			return err
		}
		for _, edge := range predicate.GetAllEndpoints() {
			relationContents = append(relationContents, []EntityKey{edge[0], predicateKey, edge[1]})
		}
	}
	rsopRelation.add3Values(subjectVar, predicateVar, objectVar, relationContents)
	cursor.join(rsopRelation, []string{predicateVar})
	return nil
}

type resolveVarTripleAll struct {
	term queryTerm
}

func (op *resolveVarTripleAll) String() string {
	return fmt.Sprintf("[resolveVarTripleAll %s]", op.term.triple)
}

func (op *resolveVarTripleAll) GetTerm() queryTerm {
	return op.term
}

func (op *resolveVarTripleAll) run(cursor *Cursor) error {
	//var (
	//	subjectVar   = op.term.Subject.String()
	//	objectVar    = op.term.Object.String()
	//	predicateVar = op.term.Predicates[0].Predicate.String()
	//)
	//var relation = newRelation([]string{subjectVar, predicateVar, objectVar})
	//var content [][]EntityKey

	//iter := func(subjectHash EntityKey, entity Entity) bool {
	//	for _, predHash := range entity.GetAllPredicates() {
	//		for _, objectHash := range entity.ListOutEndpoints(predHash) {
	//			content = append(content, []EntityKey{subjectHash, predHash, objectHash})
	//		}
	//	}
	//	return false // continue iter
	//}
	//if err := ctx.iterAllEntities(iter); err != nil {
	//	return err
	//}

	//relation.add3Values(subjectVar, predicateVar, objectVar, content)
	//if len(ctx.rel.rows) > 0 {
	//	//panic("This should not happen! Tell Gabe")
	//	logrus.Warning(subjectVar, predicateVar, objectVar)
	//	ctx.rel.join(relation, []string{subjectVar}, ctx)
	//	//ctx.markJoined(subjectVar)
	//} else {
	//	// in this case, we just replace the relation
	//	ctx.rel = relation
	//}
	//ctx.markJoined(subjectVar)
	//ctx.markJoined(predicateVar)
	//ctx.markJoined(objectVar)
	return nil
}
