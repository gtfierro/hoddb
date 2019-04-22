package loader

import (
	"os"

	logpb "git.sr.ht/~gabe/hod/proto"
	"github.com/olekukonko/tablewriter"
)

func (c *Cursor) dumpResponse(resp *logpb.Response) {
	table := tablewriter.NewWriter(os.Stdout)

	table.SetHeader(resp.Variables)
	for _, row := range resp.Rows {
		var v []string
		for _, varname := range resp.Variables {
			idx := c.variablePosition[varname]
			uri := row.Values[idx].Namespace + "#" + row.Values[idx].Value
			v = append(v, uri)
		}
		table.Append(v)
	}
	table.Render()
}
