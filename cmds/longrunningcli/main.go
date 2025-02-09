package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/longrunning-service/cmds/longrunningcli/cmds"
)

func main() {
	cli := cli.New("officehourcli")

	root := cmds.GetRootCommand(cli)
	if err := root.ExecuteContext(context.Background()); err != nil {
		slog.Error("failed to execute", "error", err)
		os.Exit(-1)
	}
}
