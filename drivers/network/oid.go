package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
)

type OIDDatabase struct {
	entries []entry
}

type entry struct {
	mac          []byte
	prefixlen    uint
	manufacturer string
}

func parseMacAddr(m string) []byte {
	if len(m) < 14 {
		num_seps := strings.Count(m, ":")
		for i := num_seps; i < 5; i++ {
			m += ":00"
		}
	}
	mac, err := net.ParseMAC(m)
	if err != nil {
		panic(err)
	}
	return []byte(mac)
}

func parseMAC(m []byte) ([]byte, uint) {
	slash_delim_idx := bytes.Index(m, []byte("/"))
	if slash_delim_idx >= 0 {
		_prefixlen, err := strconv.Atoi(string(m[slash_delim_idx+1:]))
		prefixlen := uint(_prefixlen)
		if err != nil {
			panic(err)
		}
		// :-delimited hex number
		mac := parseMacAddr(string(m[:slash_delim_idx]))

		return []byte(mac), prefixlen
	}
	num_seps := bytes.Count(m, []byte(":"))
	prefixlen := uint((num_seps + 1) * 8)
	mac := parseMacAddr(string(m))
	return []byte(mac), prefixlen
}

func NewOIDDatabase(r io.Reader) *OIDDatabase {
	s := bufio.NewScanner(r)
	oiddb := &OIDDatabase{}
	for s.Scan() {
		nextline := s.Bytes()
		if len(nextline) == 0 || nextline[0] == '#' {
			continue
		}
		fields := bytes.Split(nextline, []byte("\t"))
		var e entry
		e.mac, e.prefixlen = parseMAC(fields[0])
		if len(fields) == 2 {
			e.manufacturer = string(fields[1])
		} else {
			e.manufacturer = string(fields[2])
		}

		oiddb.entries = append(oiddb.entries, e)

		//fmt.Println(string(e.mac), e.prefixlen, e.manufacturer)
	}
	return oiddb
}

func (oid *OIDDatabase) Lookup(_mac string) string {
	var longestmatch uint = 0
	var manu = "unknown"
	mac := parseMacAddr(_mac)
	for _, ent := range oid.entries {
		bytepfx := ent.prefixlen / 8
		if bytes.Equal(mac[:bytepfx], ent.mac[:bytepfx]) {
			if ent.prefixlen > longestmatch {
				manu = ent.manufacturer
				longestmatch = ent.prefixlen
			}
		}
	}
	return manu
}
