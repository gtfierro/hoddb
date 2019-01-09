package main

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	query "git.sr.ht/~gabe/hod/lang"
	sparql "git.sr.ht/~gabe/hod/lang/ast"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/pkg/errors"
	"github.com/pkg/profile"
	logrus "github.com/sirupsen/logrus"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func init() {
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, ForceColors: true})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)
}

type Log struct {
	log       *badger.DB
	db        *badger.DB
	triggerDB *badger.DB
	versionDB *versionmanager

	cfg *Config

	// metadata for the log database
	// the graphs we have stored
	graphs []string

	pending      chan *logpb.LogEntry
	namespaces   map[string]string
	reverseEdges map[string]logpb.URI
	versions     chan int64
	// stores the most recent versions for all tags
	tagVersions map[string]int64
	cursorCache map[[2]int64]*Cursor
	sync.RWMutex
}

func NewLog(cfg *Config) (*Log, error) {

	// debug performance
	if cfg.Profile.EnableHttp {
		go func() {
			logrus.Info(http.ListenAndServe("localhost:"+cfg.Profile.HttpPort, nil))
		}()
	} else if cfg.Profile.EnableCpu {
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	} else if cfg.Profile.EnableMem {
		defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	} else if cfg.Profile.EnableBlock {
		defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
	}

	/* open log database */
	logdir := filepath.Join(cfg.Database.Path, "_log_")
	if err := os.MkdirAll(logdir, 0700); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	opts.Dir = logdir
	opts.ValueDir = logdir
	log, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	/* open view database */
	dbdir := filepath.Join(cfg.Database.Path, "_db_")
	if err := os.MkdirAll(dbdir, 0700); err != nil {
		return nil, err
	}
	opts.Dir = dbdir
	opts.ValueDir = dbdir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	L := &Log{
		log:          log,
		db:           db,
		cfg:          cfg,
		namespaces:   make(map[string]string),
		reverseEdges: make(map[string]logpb.URI),
		pending:      make(chan *logpb.LogEntry),
		tagVersions:  make(map[string]int64),
		versions:     make(chan int64, 10),
		cursorCache:  make(map[[2]int64]*Cursor),
	}
	if err := L.openTriggerDatabase(cfg); err != nil {
		return nil, err
	}

	err = L.buildVersionManager(cfg)
	if err != nil {
		logrus.Fatal(err)
	}

	// start GC on the database
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			logrus.Debug("running gc")
		againDb:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto againDb
			}
		againLog:
			err = log.RunValueLogGC(0.7)
			if err == nil {
				goto againLog
			}
		againTrig:
			err = L.triggerDB.RunValueLogGC(0.7)
			if err == nil {
				goto againTrig
			}
		}
	}()

	numWorkers := 5
	graphs := make(chan string, numWorkers)
	buildCursorWorker := func() {
		for graph := range graphs {
			s := time.Now()
			c, err := L.createCursor(graph, 0, time.Now().UnixNano())
			if err != nil {
				logrus.Fatal(errors.Wrapf(err, "Could not create cursor for graph %s", graph))
			}
			c.dropCache()
			processtime := time.Since(s)
			logrus.Infof("Processed cursor for graph %s in %s", graph, processtime)
		}
	}
	for i := 0; i < numWorkers; i++ {
		go buildCursorWorker()
	}

	//go func() {
	numBuildings := len(cfg.Database.Buildings)
	processed := 0
	for graphname, graphfile := range cfg.Database.Buildings {
		s := time.Now()
		for _, ontologyfile := range cfg.Database.Ontologies {
			if exists, _ := L.versionDB.filehashExists(ontologyfile, ontologyfile, graphname); !exists {
				_, err = L.LoadFile(graphname, ontologyfile, ontologyfile)
				if err != nil {
					logrus.Fatal(errors.Wrapf(err, "could not load file %s for graph %s", ontologyfile, graphname))
				}
				err = L.versionDB.addFileHashToTag(ontologyfile, ontologyfile, graphname)
				if err != nil {
					logrus.Fatal(errors.Wrapf(err, "could not load file %s for graph %s", graphfile, graphname))
				}
			}
		}

		if exists, _ := L.versionDB.filehashExists(graphfile, graphfile, graphname); !exists {
			_, err := L.LoadFile(graphname, graphfile, graphfile)
			if err != nil {
				logrus.Fatal(errors.Wrapf(err, "could not load file %s for graph %s", graphfile, graphname))
			}
			err = L.versionDB.addFileHashToTag(graphfile, graphfile, graphname)
			if err != nil {
				logrus.Fatal(errors.Wrapf(err, "could not load file %s for graph %s", graphfile, graphname))
			}
		}

		//c, err := L.createCursor(graphname, 0, version)
		//if err != nil {
		//	logrus.Fatal(errors.Wrapf(err, "Could not create cursor for graph %s", graphname))
		//}
		//c.dropCache()
		graphs <- graphname
		processed += 1
		processtime := time.Since(s)
		logrus.Infof("Loaded in %d/%d (%.2f%%) buildings from config file (%s took %s)", processed, numBuildings, 100*float64(processed)/float64(numBuildings), graphname, processtime)
	}
	//}()
	//for graphname := range cfg.Database.Buildings {
	//}

	return L, err

}

func (l *Log) Close() {
	close(l.pending)
	l.log.Close()
	l.db.Close()
	l.triggerDB.Close()
}

func (l *Log) readUntilTimestamp(timestamp int64) chan *logpb.LogEntry {
	var entries = make(chan *logpb.LogEntry)
	go func() {
		defer close(entries)
		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 10
		err := l.log.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				var entry = new(logpb.LogEntry)
				err := item.Value(func(b []byte) error {
					if len(b) <= 8 {
						return nil
					}
					return proto.Unmarshal(b, entry)
				})

				if err != nil {
					return err
				}
				if entry != nil {
					entries <- entry
				}
			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	return entries
}

// also respects latest tags
func (l *Log) readRangeGraph(graph string, timestamp_start, timestamp_end int64) chan *logpb.LogEntry {
	var entries = make(chan *logpb.LogEntry)
	latest, err := l.versionDB.tagsForGraphAt(graph, timestamp_end)
	for k, v := range latest {
		logrus.Warning(k, ": ", v)
	}
	if err != nil {
		logrus.Error(err)
		return entries
	}
	go func() {
		defer close(entries)
		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 10
		err := l.log.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				var entry = &logpb.LogEntry{}
				if err := item.Value(func(b []byte) error {
					if len(b) <= 8 {
						return nil
					}
					return proto.Unmarshal(b, entry)
				}); err != nil {
					return err
				}

				if entry == nil {
					continue
				}

				// check timestamp bounds, graph source, tag version
				if entry.Graph == graph &&
					//l.tagVersions[entry.Tag] == entry.Timestamp &&
					latest[entry.Tag] == entry.Timestamp &&
					entry.Timestamp >= timestamp_start &&
					entry.Timestamp <= timestamp_end {

					entries <- entry
				}

			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	return entries
}

// TODO: need to read out the timestamp from the LogEntry struct!
func (l *Log) readRange(timestamp_start, timestamp_end int64) chan *logpb.LogEntry {
	var entries = make(chan *logpb.LogEntry)
	go func() {
		defer close(entries)
		opt := badger.DefaultIteratorOptions
		err := l.log.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(opt)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				var entry = &logpb.LogEntry{}
				if err := item.Value(func(b []byte) error {
					if len(b) <= 8 {
						return nil
					}
					return proto.Unmarshal(b, entry)
				}); err != nil {
					return err
				}

				if entry == nil || entry.Timestamp == 0 {
					continue
				}

				// check timestamp bounds
				if entry.Timestamp >= timestamp_start && entry.Timestamp <= timestamp_end {
					entries <- entry
				}

			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	return entries
}

func (l *Log) GetEntity(key EntityKey) (*Entity, error) {
	var entity = &Entity{
		e:   new(logpb.Entity),
		key: key,
	}
	err := l.db.View(func(t *badger.Txn) error {
		it, err := t.Get(key.Bytes())
		if err != nil {
			return err
		}
		err = it.Value(func(b []byte) error {
			return proto.Unmarshal(b, entity.e)
		})
		if err != nil {
			return err
		}
		return nil
	})
	return entity, err
}

// ignores timestamp
func (l *Log) GetRecentEntity(key EntityKey) (entity *Entity, err error) {

	err = l.db.View(func(txn *badger.Txn) error {

		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 10

		prefix := key.Prefix()

		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := EntityKeyFromBytes(item.Key())
			fmt.Println(">", key.Timestamp())

			entity = &Entity{
				e:   new(logpb.Entity),
				key: key,
			}
			err := item.Value(func(b []byte) error {
				return proto.Unmarshal(b, entity.e)
			})
			if err != nil {
				return err
			}
			//break
		}

		return nil
	})
	return entity, err
}

func (L *Log) expand(uri *logpb.URI) *logpb.URI {
	if !strings.HasPrefix(uri.Value, "?") {
		if full, found := L.namespaces[uri.Namespace]; found {
			uri.Namespace = full
		}
	}
	return uri
}

func (L *Log) parseQuery(qstr string, version int64) (*logpb.SelectQuery, error) {
	q, err := query.Parse(qstr)
	if err != nil {
		return nil, err
	}

	sq := &logpb.SelectQuery{
		Vars:      q.Variables,
		Graphs:    q.From.Databases,
		Timestamp: version,
		Filter:    logpb.TimeFilter_Before,
		//Where:
	}

	for _, triple := range q.Where.Terms {
		term := &logpb.Triple{
			Subject: L.expand(convertURI(triple.Subject)),
			Object:  L.expand(convertURI(triple.Object)),
		}
		for _, pred := range triple.Predicates {
			// TODO: use pattern
			uri := L.expand(convertURI(pred.Predicate))
			switch pred.Pattern {
			case sparql.PATTERN_SINGLE:
				uri.Pattern = logpb.Pattern_Single
			case sparql.PATTERN_ZERO_ONE:
				uri.Pattern = logpb.Pattern_ZeroOne
			case sparql.PATTERN_ONE_PLUS:
				uri.Pattern = logpb.Pattern_OnePlus
			case sparql.PATTERN_ZERO_PLUS:
				uri.Pattern = logpb.Pattern_ZeroPlus
			}
			term.Predicate = append(term.Predicate, uri)
		}
		sq.Where = append(sq.Where, term)
	}

	return sq, nil
}

func (L *Log) parseTrigger(qstr, graph, name string) (*logpb.Trigger, error) {
	q, err := query.Parse(qstr)
	if err != nil {
		return nil, err
	}

	trigger := &logpb.Trigger{
		Graph: graph,
		Name:  name,
		//Where:
	}

	for _, triple := range q.Where.Terms {
		term := &logpb.Triple{
			Subject: L.expand(convertURI(triple.Subject)),
			Object:  L.expand(convertURI(triple.Object)),
		}
		for _, pred := range triple.Predicates {
			// TODO: use pattern
			uri := L.expand(convertURI(pred.Predicate))
			switch pred.Pattern {
			case sparql.PATTERN_SINGLE:
				uri.Pattern = logpb.Pattern_Single
			case sparql.PATTERN_ZERO_ONE:
				uri.Pattern = logpb.Pattern_ZeroOne
			case sparql.PATTERN_ONE_PLUS:
				uri.Pattern = logpb.Pattern_OnePlus
			case sparql.PATTERN_ZERO_PLUS:
				uri.Pattern = logpb.Pattern_ZeroPlus
			}
			term.Predicate = append(term.Predicate, uri)
		}
		trigger.Where = append(trigger.Where, term)
	}

	for _, triple := range q.Insert.Terms {
		term := &logpb.Triple{
			Subject: L.expand(convertURI(triple.Subject)),
			Object:  L.expand(convertURI(triple.Object)),
		}
		for _, pred := range triple.Predicates {
			// TODO: use pattern
			uri := L.expand(convertURI(pred.Predicate))
			switch pred.Pattern {
			case sparql.PATTERN_SINGLE:
				uri.Pattern = logpb.Pattern_Single
			case sparql.PATTERN_ZERO_ONE:
				uri.Pattern = logpb.Pattern_ZeroOne
			case sparql.PATTERN_ONE_PLUS:
				uri.Pattern = logpb.Pattern_OnePlus
			case sparql.PATTERN_ZERO_PLUS:
				uri.Pattern = logpb.Pattern_ZeroPlus
			}
			term.Predicate = append(term.Predicate, uri)
		}
		trigger.Insert = append(trigger.Insert, term)
	}

	return trigger, nil
}
