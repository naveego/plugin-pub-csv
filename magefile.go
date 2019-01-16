// +build mage

package main

import (
	"github.com/naveego/ci/go/build"
	"github.com/naveego/dataflow-contracts/plugins"
	"github.com/naveego/plugin-pub-csv/version"
	"os"
)

func Build() error {
	cfg := build.PluginConfig{
		Package: build.Package{
			VersionString: version.Version.String(),
			PackagePath:   "github.com/naveego/plugin-pub-csv",
			Name:          "pub-csv",
			Shrink:        true,
		},
		Targets: []build.PackageTarget{
			build.TargetLinuxAmd64,
			build.TargetDarwinAmd64,
			build.TargetWindowsAmd64,
		},
	}

	err := build.BuildPlugin(cfg)
	return err
}


func PublishBlue() error {
	os.Setenv("UPLOAD", "blue")
	return Build()
}

func GenerateGRPC() error {
	destDir := "./internal/pub"
	return plugins.GeneratePublisher(destDir)
}
