package loader

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
	//seen := newEntitySet()
	next := newEntityStack()
	results := newEntitySet()
	//sequence = reversePath(sequence)
	for idx := len(sequence) - 1; idx >= 0; idx-- {
		//for idx, segment := range sequence {
		segment := sequence[idx]
		//logrus.Println(segment)
		for next.len() > 0 {
			object := next.pop()
			stack.push(object)
		}
		for stack.len() > 0 {
			object := stack.pop()
			nexthop, _, err := cursor.followPathFromObject(object, segment)
			if err != nil {
				return nil, err
			}

			//seen.addFrom(traversed)

			for key := range nexthop {
				//if idx == len(sequence)-1 {
				if idx == 0 {
					results.add(key)
				}
				// put results back on stack if we aren't done traversing
				//if idx < len(sequence)-1 {
				if idx > 0 {
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
	for _, endpoint := range pred.compiled.Endpoints {
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

type entitystack struct {
	entities []*Entity
}

func newEntityStack() *entitystack {
	return &entitystack{}
}

func (stack *entitystack) push(e *Entity) {
	stack.entities = append(stack.entities, e)
}

func (stack *entitystack) pop() *Entity {
	if len(stack.entities) == 0 {
		return nil
	}
	e := stack.entities[0]
	stack.entities = stack.entities[1:]
	return e
}

func (stack *entitystack) len() int {
	return len(stack.entities)
}

type entityset map[EntityKey]struct{}

func newEntitySet() entityset {
	return entityset(make(map[EntityKey]struct{}))
}

func (set entityset) has(e EntityKey) bool {
	_, found := set[e]
	return found
}

func (set entityset) addIfNotHas(e EntityKey) bool {
	if _, found := set[e]; found {
		return true
	}
	set[e] = struct{}{}
	return false
}

func (set entityset) addFrom(other entityset) {
	for k := range other {
		set.add(k)
	}
}

func (set entityset) add(e EntityKey) {
	set[e] = struct{}{}
}
