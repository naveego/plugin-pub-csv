// Copyright © 2017 Naveego

package cmd

import (
	"fmt"
	"os"

	"github.com/naveego/navigator-go/publishers/server"
	"github.com/naveego/plugin-pub-csv/csv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var verbose *bool

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "plugin-pub-csv",
	Short: "A publisher that pulls data from a CSV file.",
	Args:  cobra.ExactArgs(1),
	Long:  `Runs the publisher in externally controlled mode.`,
	RunE: func(cmd *cobra.Command, args []string) error {

		logrus.SetOutput(os.Stdout)

		addr := args[0]

		if *verbose {
			fmt.Println("Verbose logging")
			logrus.SetLevel(logrus.DebugLevel)
		}

		publisher := csv.NewServer()

		srv := server.NewPublisherServer(addr, publisher)

		err := srv.ListenAndServe()

		return err
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
