package hod

import (
	turtle "git.sr.ht/~gabe/hod/turtle"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/pkg/errors"
	"github.com/pkg/profile"
	logrus "github.com/sirupsen/logrus"

	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// initializing the Hod DB

var log = logrus.New()

func init() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, ForceColors: true})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.DebugLevel)
}

type hashkeyentry struct {
	Graph string
	Uri   turtle.URI
}

type HodDB struct {
	db *badger.DB
	//versionDB *versionmanager
	cfg *Config

	hashes map[hashkeyentry]EntityKey
	uris   map[EntityKey]turtle.URI
	sync.RWMutex

	// TODO: serialize/deserialize
	// map graph name to namespaces (map[string]map[string]string)
	namespaces sync.Map
	graphs     map[string]struct{}
}

func (db *HodDB) Backup(w io.Writer) error {

	// write the hashes, uris to the store
	hashpfx := []byte("hashpfx")
	db.Lock()
	defer db.Unlock()

	// Backup db.hashes
	wb := db.db.NewWriteBatch()
	defer wb.Cancel()
	for hashkey, entitykey := range db.hashes {
		serializedkey, err := json.Marshal(hashkey)
		if err != nil {
			return err
		}
		var b []byte
		b = append(b, hashpfx...)
		b = append(b, serializedkey...)
		if err := wb.Set(b, entitykey.Bytes()); err != nil {
			return err
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}

	// Backup db.uris
	entitypfx := []byte("entitypfx")
	wb2 := db.db.NewWriteBatch()
	defer wb2.Cancel()

	for entitykey, uri := range db.uris {
		serializeduri, err := json.Marshal(uri)
		if err != nil {
			return err
		}
		var b []byte
		b = append(b, entitypfx...)
		b = append(b, entitykey.Bytes()...)
		if err := wb2.Set(b, serializeduri); err != nil {
			return err
		}
	}
	if err := wb2.Flush(); err != nil {
		return err
	}

	// Backup db.namespaces
	nspfx := []byte("namespacepfx")
	wb3 := db.db.NewWriteBatch()
	defer wb3.Cancel()
	db.namespaces.Range(func(key, value interface{}) bool {
		// (map[string]map[string]string)
		//key
		var b []byte
		b = append(b, nspfx...)
		b = append(b, []byte(key.(string))...)
		//value
		serialized, err := json.Marshal(value)
		if err != nil {
			log.Error(err)
			return false
		}

		if err := wb3.Set(b, serialized); err != nil {
			log.Error(err)
			return false
		}

		return true
	})
	if err := wb3.Flush(); err != nil {
		return err
	}

	_, err := db.db.Backup(w, 0)
	return err
}

func MakeHodDB(cfg *Config) (*HodDB, error) {
	// handle profiling
	if cfg.Profile.EnableCpu {
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	} else if cfg.Profile.EnableMem {
		defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	} else if cfg.Profile.EnableBlock {
		defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
	}
	// debug performance
	if cfg.Profile.EnableHttp {
		go func() {
			log.Info("Profile at localhost:", cfg.Profile.HttpPort)
			log.Info(http.ListenAndServe("localhost:"+cfg.Profile.HttpPort, nil))
		}()
	}

	/* open view database */
	dbdir := filepath.Join(cfg.Database.Path, "_db_")
	if err := os.MkdirAll(dbdir, 0700); err != nil {
		return nil, errors.Wrap(err, "Could not make _db_")
	}
	opts := badger.DefaultOptions(dbdir)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "Could not open badger db")
	}

	hod := &HodDB{
		db:     db,
		cfg:    cfg,
		hashes: make(map[hashkeyentry]EntityKey),
		uris:   make(map[EntityKey]turtle.URI),
		graphs: make(map[string]struct{}),
	}

	//err = hod.buildVersionManager(cfg)
	//if err != nil {
	//	log.Fatal(err)
	//}

	// start GC on the database
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
		againDb:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto againDb
			}
		}
	}()

	numBuildings := len(cfg.Database.Buildings)

	processed := 0
	for graphname, graphfile := range cfg.Database.Buildings {
		bundle := FileBundle{
			GraphName:     graphname,
			TTLFile:       graphfile,
			OntologyFiles: cfg.Database.Ontologies,
		}
		s := time.Now()
		if err := hod.Load(bundle); err != nil {
			log.Error(errors.Wrapf(err, "Could not load file %s", graphname))
		}
		processtime := time.Since(s)
		processed += 1
		log.Infof("Loaded in %d/%d (%.2f%%) buildings from config file (%s took %s)", processed, numBuildings, 100*float64(processed)/float64(numBuildings), graphname, processtime)
	}

	return hod, nil
}

func MakeHodDBLambda(cfg *Config, backup io.Reader) (*HodDB, error) {

	//dbdir := filepath.Join(cfg.Database.Path, "/tmp/_db_")
	opts := badger.DefaultOptions("/tmp/_db_")
	opts.TableLoadingMode = options.LoadToRAM
	opts.ValueLogLoadingMode = options.MemoryMap
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "Could not open badger db")
	}
	if err := db.Load(backup, 100); err != nil {
		return nil, errors.Wrap(err, "Could not restore backup")
	}

	hod := &HodDB{
		db:     db,
		cfg:    cfg,
		hashes: make(map[hashkeyentry]EntityKey),
		uris:   make(map[EntityKey]turtle.URI),
		graphs: make(map[string]struct{}),
	}

	// read in the hash, entity keys
	db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("hashpfx")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				//fmt.Printf("key=%s, value=%s\n", k, v)
				serializedhashkey := k[len(prefix):]
				var hashkey hashkeyentry
				if err := json.Unmarshal(serializedhashkey, &hashkey); err != nil {
					return err
				}

				entitykey := EntityKeyFromBytes(v)
				hod.hashes[hashkey] = entitykey

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("entitypfx")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				//fmt.Printf("key=%s, value=%s\n", k, v)
				entitykey := EntityKeyFromBytes(k[len(prefix):])
				var uri turtle.URI
				if err := json.Unmarshal(v, &uri); err != nil {
					return err
				}

				hod.uris[entitykey] = uri

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("namespacepfx")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			// populate hod.namespaces
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				key := string(k[len(prefix):])
				var nsmap = make(map[string]string)
				if err := json.Unmarshal(v, &nsmap); err != nil {
					return err
				}

				hod.namespaces.Store(key, nsmap)
				hod.graphs[key] = struct{}{}

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
		againDb:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				goto againDb
			}
		}
	}()

	hod.namespaces.Range(func(k, v interface{}) bool {
		fmt.Println(k, v)
		return true
	})
	//	hod.namespaces.Store("ciee", map[string]string{
	//		"bf":    "https://brickschema.org/schema/1.0.3/BrickFrame",
	//		"bldg":  "http://xbos.io/ontologies/ciee",
	//		"brick": "https://brickschema.org/schema/1.0.3/Brick",
	//		"owl":   "http://www.w3.org/2002/07/owl",
	//		"rdf":   "http://www.w3.org/1999/02/22-rdf-syntax-ns",
	//		"rdfs":  "http://www.w3.org/2000/01/rdf-schema",
	//		"xml":   "http://www.w3.org/XML/1998/namespace",
	//		"xsd":   "http://www.w3.org/2001/XMLSchema",
	//	})
	//	hod.graphs["ciee"] = struct{}{}

	return hod, nil
}
