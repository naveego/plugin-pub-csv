// Copyright © 2017 Naveego

package cmd

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"os"

	"github.com/naveego/plugin-pub-csv/internal"
	"github.com/naveego/plugin-pub-csv/version"
	"github.com/spf13/cobra"
	"log"
	"github.com/hashicorp/go-plugin"
	"github.com/naveego/dataflow-contracts/plugins"
	"github.com/naveego/plugin-pub-csv/internal/pub"
)

var verbose *bool

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "plugin-pub-csv",
	Short: "A publisher that pulls data from a CSV file.",
	Long: fmt.Sprintf(`Version %s
Runs the publisher in externally controlled mode.`, version.Version.String()),
	Run: func(cmd *cobra.Command, args []string)  {

		log.Print("Starting CSV Publisher Plugin.")
		plugin.Serve(&plugin.ServeConfig{
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion: plugins.PublisherProtocolVersion,
				MagicCookieKey:plugins.PublisherMagicCookieKey,
				MagicCookieValue:plugins.PublisherMagicCookieValue,
			},
			Plugins: map[string]plugin.Plugin{
				"publisher": pub.NewServerPlugin(internal.NewServer(
					hclog.New(&hclog.LoggerOptions{
						Level:      hclog.Trace,
						Output:     os.Stderr,
						JSONFormat: true,
					}))),
			},

			// A non-nil value here enables gRPC serving for this plugin...
			GRPCServer: plugin.DefaultGRPCServer,
		})
	}}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	verbose = RootCmd.Flags().BoolP("verbose", "v", false, "enable verbose logging")
}
