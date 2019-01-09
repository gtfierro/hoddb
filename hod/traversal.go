package hod

import (
	logpb "git.sr.ht/~gabe/hod/proto"
)

type edge struct {
	predicate EntityKey
	pattern   logpb.Pattern
}

// traversing the log graph
func (cursor *Cursor) followPathFromObject(object *Entity, e edge) (entityset, entityset, error) {
	stack := newEntityStack()
	stack.push(object)

	seen := newEntitySet()
	results := newEntitySet()

	for stack.len() > 0 {
		object := stack.pop()

		// skip if already seen, else add to traversed
		if seen.addIfNotHas(object.key) {
			continue
		}

		switch e.pattern {
		case logpb.Pattern_ZeroOne:
			results.add(object.key)
			fallthrough
		case logpb.Pattern_Single:
			for _, subjectKey := range object.InEdges(e.predicate) {
				results.add(subjectKey)
			}

		case logpb.Pattern_ZeroPlus:
			results.add(object.key)
			fallthrough
		case logpb.Pattern_OnePlus:
			for _, subjectKey := range object.InPlusEdges(e.predicate) {
				results.add(subjectKey)
				subject, err := cursor.getEntity(subjectKey)
				if err != nil {
					return nil, nil, err
				}
				stack.push(subject)
			}
		}

	}
	return results, seen, nil
}

func (cursor *Cursor) followPathFromSubject(subject *Entity, e edge) (entityset, entityset, error) {
	stack := newEntityStack()
	stack.push(subject)

	seen := newEntitySet()
	results := newEntitySet()

	for stack.len() > 0 {
		subject := stack.pop()
		// skip if already seen, else add to traversed
		if seen.addIfNotHas(subject.key) {
			continue
		}

		switch e.pattern {
		case logpb.Pattern_ZeroOne:
			results.add(subject.key)
			fallthrough
		case logpb.Pattern_Single:
			for _, objectKey := range subject.OutEdges(e.predicate) {
				results.add(objectKey)
			}

		case logpb.Pattern_ZeroPlus:
			results.add(subject.key)
			fallthrough
		case logpb.Pattern_OnePlus:
			for _, objectKey := range append(subject.OutPlusEdges(e.predicate), subject.OutEdges(e.predicate)...) {
				results.add(objectKey)
				object, err := cursor.getEntity(objectKey)
				if err != nil {
					return nil, nil, err
				}
				stack.push(object)
			}
		}

	}
	return results, seen, nil
}

func (cursor *Cursor) getSubjectFromPredObject(object *Entity, sequence []edge) (entityset, error) {
	stack := newEntityStack()
	stack.push(object)
	seen := newEntitySet()
	next := newEntityStack()
	results := newEntitySet()
	//sequence = reversePath(sequence)
	for idx, segment := range sequence {
		for next.len() > 0 {
			stack.push(next.pop())
		}
		for stack.len() > 0 {
			object := stack.pop()
			if seen.addIfNotHas(object.key) {
				continue
			}
			nexthop, traversed, err := cursor.followPathFromObject(object, segment)
			if err != nil {
				return nil, err
			}

			seen.addFrom(traversed)

			for key := range nexthop {
				if idx == len(sequence)-1 {
					results.add(key)
				}
				// put results back on stack if we aren't done traversing
				if idx < len(sequence)-1 {
					object, err := cursor.getEntity(key)
					if err != nil {
						return nil, err
					}
					next.push(object)
				}
			}
		}
	}
	return results, nil
}

func (cursor *Cursor) getObjectFromSubjectPred(subject *Entity, sequence []edge) (entityset, error) {
	stack := newEntityStack()
	stack.push(subject)

	seen := newEntitySet()
	next := newEntityStack()

	results := newEntitySet()

	for idx, segment := range sequence {
		for next.len() > 0 {
			stack.push(next.pop())
		}
		for stack.len() > 0 {
			subject := stack.pop()
			if seen.addIfNotHas(subject.key) {
				continue
			}
			nexthop, traversed, err := cursor.followPathFromSubject(subject, segment)
			if err != nil {
				return nil, err
			}

			seen.addFrom(traversed)

			for key := range nexthop {
				if idx == len(sequence)-1 {
					results.add(key)
				}
				// put results back on stack if we aren't done traversing
				if idx < len(sequence)-1 {
					subject, err := cursor.getEntity(key)
					if err != nil {
						return nil, err
					}
					next.push(subject)
				}
			}
		}
	}

	return results, nil
}

func (cursor *Cursor) getSubjectObjectFromPred(sequence edge) (sos [][]EntityKey, err error) {
	pred, err := cursor.getEntity(sequence.predicate)
	if err != nil {
		err = nil
		return
	}
	for _, endpoint := range pred.e.Endpoints {
		pair := []EntityKey{EntityKeyFromBytes(endpoint.Src), EntityKeyFromBytes(endpoint.Dst)}
		sos = append(sos, pair)
	}
	return
}

func (cursor *Cursor) getPredicateFromSubjectObject(subjectKey, objectKey EntityKey) (entityset, error) {
	results := newEntitySet()
	subject, err := cursor.getEntity(subjectKey)
	if err != nil {
		return nil, err
	}
	for _, pred := range subject.GetAllPredicates() {
		for _, testObject := range subject.OutEdges(pred) {
			if testObject == objectKey {
				results.add(pred)
			}
		}
	}
	return results, nil
}

func (cursor *Cursor) getPredicatesFromObject(objectKey EntityKey) ([]EntityKey, error) {
	object, err := cursor.getEntity(objectKey)
	if err != nil {
		return nil, err
	}
	return object.GetAllPredicates(), nil
}

func (cursor *Cursor) getPredicatesFromSubject(subjectKey EntityKey) ([]EntityKey, error) {
	subject, err := cursor.getEntity(subjectKey)
	if err != nil {
		return nil, err
	}
	return subject.GetAllPredicates(), nil
}

func (cursor *Cursor) iterAllEntities(F func(EntityKey, *Entity) bool) error {
	return cursor.Iterate(F)
}
