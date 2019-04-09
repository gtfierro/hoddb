package hod

import (
	"crypto/sha256"
	"database/sql"
	logpb "git.sr.ht/~gabe/hod/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

/*
When we read through the log, we will know the version and graph for each
item that is added. Need to design the api for this.

When the database boots, we can read through the whole log to build up the set of versions.
*/

type versionmanager struct {
	db *sql.DB
}

// read through the whole log in order to build up a notion of what versions are there
func (L *Log) buildVersionManager(cfg *Config) error {
	var err error
	vm := &versionmanager{}

	vm.db, err = sql.Open("sqlite3", filepath.Join(cfg.Database.Path, "versions.sqlite3"))
	if err != nil {
		return err
	}

	// create sql table for vrsions
	sqlStmt := `
    create table if not exists versions (version integer not null primary key, graph text, tag text, sourcehash bytes);
    `
	_, err = vm.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	_, err = vm.db.Exec("CREATE TABLE IF NOT EXISTS namespaces (namespace text not null unique, abbr text);")
	if err != nil {
		return err
	}

	currentTime := time.Now().UnixNano()
	entries := L.readRange(0, currentTime)
	for entry := range entries {
		if entry.Op != logpb.Op_ADD {
			continue
		}
		_, _, err = vm.addEntry(entry)
		if err != nil {
			return errors.Wrap(err, "addentry")
		}
	}
	L.versionDB = vm

	return nil
}

func (vm *versionmanager) listAllGraphs() (graphs []string, err error) {
	rows, rerr := vm.db.Query("SELECT distinct graph from versions;")
	if rerr != nil {
		err = rerr
		return
	}
	defer rows.Close()
	for rows.Next() {
		var graph string
		err = rows.Scan(&graph)
		if err != nil {
			return
		}
		graphs = append(graphs, graph)
	}
	return
}

// add the version/graph combo to the graph if it doesn't already exist
// return true if we're adding a new version
func (vm *versionmanager) addEntry(entry *logpb.LogEntry) (newversion bool, newgraph bool, retErr error) {
	newversion = false
	newgraph = false

	// begin transaction
	tx, err := vm.db.Begin()
	if err != nil {
		retErr = err
		return
	}
	defer tx.Rollback()

	// what are the most recent version for each tag on this graph
	prepared, err := tx.Prepare("SELECT MAX(version), tag FROM versions WHERE graph = ? AND version <= ? GROUP BY tag;")
	if err != nil {
		retErr = err
		return
	}

	rows, err := prepared.Query(entry.Graph, entry.Timestamp)
	if err != nil {
		retErr = err
		return
	}
	defer rows.Close()

	// if we have rows, then find the most recent version number for this tag
	maxversion := int64(-1)
	newgraph = true
	for rows.Next() {
		newgraph = false
		var _version int64
		var _tag string
		err = rows.Scan(&_version, &_tag)
		if err != nil {
			retErr = err
		}

		if _version > maxversion && _tag == entry.Tag {
			maxversion = _version
		}
	}
	newversion = (maxversion > -1 && maxversion < entry.Timestamp)

	// if we have a new graph, or the most recent found version is still before our timestamp,
	// then we insert a new version and tag into the database
	if newgraph || newversion || maxversion == -1 {
		newversion = true
		istmt, err := tx.Prepare("INSERT INTO versions(version, graph, tag) VALUES (?, ?, ?);")
		if err != nil {
			retErr = err
			return
		}
		if _, err := istmt.Exec(entry.Timestamp, entry.Graph, entry.Tag); err != nil {
			retErr = err
			return
		}
		retErr = tx.Commit()
	}

	return
}

// pulls the most recent version (<= the given timestamp) of each tag for the given graph
// returns map of tag => version # for that tag
func (vm *versionmanager) tagsForGraphAt(graph string, timestamp int64) (map[string]int64, error) {
	prepared, err := vm.db.Prepare("SELECT MAX(version), tag FROM versions WHERE graph = ? AND version <= ? GROUP BY tag;")
	if err != nil {
		return nil, err
	}

	rows, err := prepared.Query(graph, timestamp)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := make(map[string]int64)
	for rows.Next() {
		var _version int64
		var _tag string
		err = rows.Scan(&_version, &_tag)
		ret[_tag] = _version
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// returns the timestamp of the most recent version for the graph.
// We use the most recent version so we can point to the exact key for an entity
// instead of having to do a scan for it.
// TODO: handle graph not found
func (vm *versionmanager) latestVersion(graph string, before int64) (int64, error) {
	prepared, err := vm.db.Prepare("SELECT MAX(version) FROM versions WHERE graph = ? AND version <= ?;")
	if err != nil {
		return -1, err
	}
	var _ver int64
	row := prepared.QueryRow(graph, before)
	err = row.Scan(&_ver)
	return _ver, err
}

// compute the file hash of the given file name. If the most recent version of the tag
// for the graph has this file hash, then we don't need to load the file.
func (vm *versionmanager) addFileHashToTag(filename, tag, graph string, version int64) error {
	stmt, err := vm.db.Prepare("UPDATE versions SET sourcehash=? WHERE graph=? AND  tag=? AND version=?")
	if err != nil {
		return err
	}
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return err
	}
	filehasher := sha256.New()
	filehash := filehasher.Sum(nil)

	_, err = stmt.Exec(filehash, graph, tag, version)
	if err != nil {
		return err
	}

	return nil
}

// returns list of graphs, list of versions
func (vm *versionmanager) listGraphs(graphs []string, version int64) ([]string, []int64, error) {
	if len(graphs) == 0 {
		return nil, nil, nil
	}

	var rows *sql.Rows
	var prepared *sql.Stmt
	var err error

	if graphs[0] == "*" {
		prepared, err = vm.db.Prepare("SELECT MAX(version), graph FROM versions WHERE version <= ? GROUP BY graph;")
		if err != nil {
			return nil, nil, err
		}
		rows, err = prepared.Query(version)
	} else {
		// TODO:...
		prepared, err = vm.db.Prepare("SELECT MAX(version), graph FROM versions WHERE version <= ? and graph IN (?) GROUP BY graph;")
		if err != nil {
			return nil, nil, err
		}
		rows, err = prepared.Query(version, graphs)
	}

	if err != nil {
		return nil, nil, err
	}

	defer rows.Close()
	var retgraphs []string
	var retversions []int64
	for rows.Next() {
		var _version int64
		var _graph string
		err = rows.Scan(&_version, &_graph)
		if err != nil {
			return retgraphs, retversions, err
		}
		retgraphs = append(retgraphs, _graph)
		retversions = append(retversions, _version)
	}
	return retgraphs, retversions, err

}

func (vm *versionmanager) filehashExists(filename string, tag string, graph string) (bool, error) {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return false, err
	}
	filehasher := sha256.New()
	filehash := filehasher.Sum(nil)

	stmt, err := vm.db.Prepare("SELECT version FROM versions WHERE graph=? AND tag=? AND sourcehash=?")
	if err != nil {
		return false, err
	}
	var _ver int64
	row := stmt.QueryRow(graph, tag, filehash)
	err = row.Scan(&_ver)

	return _ver != 0, err
}

//
////func (vm *versionmanager) versionsAt(graph,

func (vm *versionmanager) addNamespace(ns, abbr string) error {
	_, err := vm.db.Exec("INSERT INTO namespaces(namespace, abbr) VALUES (?, ?);", ns, abbr)
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return nil
	}
	return err
}

func (vm *versionmanager) listNamespaces() ([][]string, error) {
	rows, err := vm.db.Query("SELECT namespace, abbr FROM namespaces;")
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var results [][]string
	for rows.Next() {
		var ns string
		var abbr string
		err = rows.Scan(&ns, &abbr)
		if err != nil {
			return nil, err
		}
		results = append(results, []string{ns, abbr})
	}
	return results, nil
}
