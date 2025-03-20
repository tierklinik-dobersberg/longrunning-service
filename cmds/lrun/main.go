package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/longrunning-service/cmds/lrun/cmds"
)

func main() {
	cli := cli.New("lrun")

	cmds.GetRootCommand(cli)

	if err := cli.ExecuteContext(context.Background()); err != nil {
		slog.Error("failed to execute", "error", err)
		os.Exit(-1)
	}
}
