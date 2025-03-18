package cmds

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/google/shlex"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

func GetRootCommand(root *cli.Root) *cobra.Command {
	// execution environment
	var (
		shell     = "/bin/bash"
		shellArgs = "-c"
		dropEnv   bool
		keepEnv   []string
	)

	// long-running data
	var (
		kind        string
		owner       string
		creator     string
		ttl         time.Duration
		gracePeriod time.Duration
	)

	cmd := &cobra.Command{
		Use:  "lrun [flags] -- command",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			parsedArgs, err := shlex.Split(shellArgs)
			if err != nil {
				logrus.Fatalf("failed to parse shell arguments: %s", err)
			}

			parsedArgs = append(parsedArgs, args[0])

			stdout := new(bytes.Buffer)
			stderr := new(bytes.Buffer)

			c := exec.CommandContext(root.Context(), shell, parsedArgs...)

			c.Stdout = io.MultiWriter(stdout, os.Stdout)
			c.Stderr = io.MultiWriter(stderr, os.Stderr)

			if dropEnv {
				env := make([]string, 0)

				for _, e := range os.Environ() {
					name, _, found := strings.Cut(e, "=")

					if !found {
						continue
					}

					if slices.Contains(keepEnv, name) {
						env = append(env, e)
					}
				}

				c.Env = env
			}

			if err := c.Run(); err != nil {
				logrus.Fatalf("failed to execute command: %s", err)
			}
		},
	}

	f := cmd.Flags()
	{
		f.StringVar(&shell, "shell", "/bin/bash", "The shell to use when executing the command")
		f.StringVar(&shellArgs, "shell-args", "-c", "The arguments to pass to the shell")

		f.BoolVar(&dropEnv, "drop-env", false, "Drop all environment variables expect those specified by --keep-env")
		f.StringSliceVar(&keepEnv, "keep-env", nil, "Environment variables to keep when using --drop-env")

		f.StringVar(&kind, "kind", "", "The kind of the long-running operation")
		f.StringVar(&owner, "owner", "", "The owner of the long-running operation")
		f.StringVar(&creator, "creator", "", "The creator of the long-running operation")
		f.DurationVar(&ttl, "ttl", 0, "The TTL for the long-running operation")
		f.DurationVar(&gracePeriod, "grace-period", 0, "The grace-period for the long running operation")
	}

	cmd.AddGroup()

	return cmd
}
