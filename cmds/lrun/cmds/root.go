package cmds

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/google/shlex"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func GetRootCommand(root *cli.Root) {
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
		description string
		creator     string
		ttl         time.Duration
		gracePeriod time.Duration
	)

	root.Use = "run [flags] -- command"
	root.Args = cobra.ExactArgs(1)
	root.Run = func(cmd *cobra.Command, args []string) {
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

		cli := root.LongRunning()

		var (
			ttlpb         *durationpb.Duration
			gracePeriodPb *durationpb.Duration
		)

		if ttl != 0 {
			ttlpb = durationpb.New(ttl)
		}

		if gracePeriod != 0 {
			gracePeriodPb = durationpb.New(gracePeriod)
		}

		regReq := &longrunningv1.RegisterOperationRequest{
			Owner:        owner,
			Creator:      creator,
			Ttl:          ttlpb,
			GracePeriod:  gracePeriodPb,
			Description:  description,
			Kind:         kind,
			InitialState: longrunningv1.OperationState_OperationState_RUNNING,
			Parameters: map[string]*structpb.Value{
				"command":   structpb.NewStringValue(args[0]),
				"shell":     structpb.NewStringValue(shell),
				"shellArgs": structpb.NewStringValue(shellArgs),
			},
		}

		res, err := cli.RegisterOperation(root.Context(), connect.NewRequest(regReq))
		if err != nil {
			logrus.Fatal(err.Error())
		}
		logrus.Infof("operation registered successfully: id=%s token=%s", res.Msg.Operation.UniqueId, res.Msg.AuthToken)

		ctx, cancel := context.WithCancel(root.Context())
		defer cancel()

		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(res.Msg.Operation.Ttl.AsDuration()):
				}

				_, err := cli.UpdateOperation(ctx, connect.NewRequest(&longrunningv1.UpdateOperationRequest{
					UniqueId:  res.Msg.Operation.UniqueId,
					AuthToken: res.Msg.GetAuthToken(),
					Running:   true,
					UpdateMask: &fieldmaskpb.FieldMask{
						Paths: []string{"running"},
					},
				}))
				if err != nil {
					logrus.Errorf("failed to update operation: %s", err)
				}
			}
		}()

		err = c.Run()
		cancel()

		wg.Wait()

		req := &longrunningv1.CompleteOperationRequest{
			UniqueId:  res.Msg.GetOperation().GetUniqueId(),
			AuthToken: res.Msg.GetAuthToken(),
		}

		if err == nil {
			req.Result = &longrunningv1.CompleteOperationRequest_Success{
				Success: &longrunningv1.OperationSuccess{
					Message: stdout.String(),
				},
			}
		} else {
			req.Result = &longrunningv1.CompleteOperationRequest_Error{
				Error: &longrunningv1.OperationError{
					Message: stderr.String(),
				},
			}
		}

		if _, err := cli.CompleteOperation(root.Context(), connect.NewRequest(req)); err != nil {
			logrus.Fatalf("failed to mark operation as complete: %s", err.Error())
		}
	}

	f := root.Flags()
	{
		f.StringVar(&shell, "shell", "/bin/bash", "The shell to use when executing the command")
		f.StringVar(&shellArgs, "shell-args", "-c", "The arguments to pass to the shell")

		f.BoolVar(&dropEnv, "drop-env", false, "Drop all environment variables expect those specified by --keep-env")
		f.StringSliceVar(&keepEnv, "keep-env", nil, "Environment variables to keep when using --drop-env")

		f.StringVarP(&kind, "kind", "k", "", "The kind of the long-running operation")
		f.StringVarP(&owner, "owner", "o", "", "The owner of the long-running operation")
		f.StringVarP(&creator, "creator", "C", "", "The creator of the long-running operation")
		f.DurationVar(&ttl, "ttl", 0, "The TTL for the long-running operation")
		f.DurationVar(&gracePeriod, "grace-period", 0, "The grace-period for the long running operation")
		f.StringVarP(&description, "description", "d", "", "An optional description")
	}
}
