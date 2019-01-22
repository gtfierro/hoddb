package hod

import (
	"context"
	"fmt"
	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestVersionsDB(t *testing.T) {
	require := require.New(t)
	dir, err := ioutil.TempDir("", "_log_test_")
	require.NoError(err)
	defer os.RemoveAll(dir) // clean up

	cfgStr := fmt.Sprintf(`
database:
    path: %s
    `, dir)
	cfg, err := ReadConfigFromString(cfgStr)
	require.NoError(err, "read config")
	require.NotNil(cfg, "config")

	L, err := NewLog(cfg)
	require.NoError(err, "open log")
	require.NotNil(L, "log")
	defer L.Close()

	version, err := L.LoadFile("test", "BrickFrame.ttl", "bf")
	require.NoError(err, "load brickframe")
	version, err = L.LoadFile("test", "Brick.ttl", "brick")
	require.NoError(err, "load brick")
	version, err = L.LoadFile("test", "example.ttl", "ex")
	require.NoError(err, "load file")

	query := &logpb.VersionQuery{
		Graphs:    []string{"*"},
		Timestamp: version,
	}
	resp, err := L.Versions(context.Background(), query)
	require.NoError(err, "run version query")
	require.NotNil(resp, "version query response")
	require.Equal(resp.Rows, []*logpb.Row{
		&logpb.Row{
			Values: []*logpb.URI{
				&logpb.URI{Value: "test"},
				&logpb.URI{Value: fmt.Sprintf("%d", version)},
			},
		},
	})
}
