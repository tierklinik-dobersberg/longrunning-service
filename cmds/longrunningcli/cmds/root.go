package cmds

import (
	"github.com/spf13/cobra"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

func GetRootCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "office-hours [command]",
		Aliases: []string{"officehours", "open-hours", "openhours", "oh"},
	}

	return cmd
}
