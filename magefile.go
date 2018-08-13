// +build mage

package main

import (
	"fmt"
	"os"
	"github.com/magefile/mage/sh"
	"github.com/naveego/dataflow-contracts/plugins"

	"github.com/naveego/ci/go/build"
	"encoding/json"
	"path/filepath"
	"io/ioutil"
	"encoding/base64"
	"github.com/naveego/plugin-pub-csv/version"
)

var oses = []string{
	"windows",
	"linux",
	"darwin",
}

// Default target to run when none is specified
// If not set, running mage will list available targets
// var Default = Build

// A build step that requires additional params, or platform specific steps for example
func Build() error {
	fmt.Println("Building...")
	for _, os := range oses {
		if err := buildForOS(os); err != nil {
			return err
		}
	}
	return nil
}

func buildForOS(os string) error {
	fmt.Println("Building for OS", os)

	manifestBytes, err := ioutil.ReadFile("manifest.json")
	if err != nil {
		return err
	}
	var manifest map[string]interface{}
	err = json.Unmarshal(manifestBytes, &manifest)
	if err != nil {
		return err
	}

	v := version.Version
	manifest["version"] = v

	exe := "pub-csv"
	if os == "windows" {
		exe += ".exe"
	}
	manifest["executable"] = exe

	outDir := fmt.Sprintf("build/outputs/%s/pub-csv/%s", os, v)
	out := filepath.Join(outDir, exe)

	err = sh.RunWith(map[string]string{
		"GOOS": os,
	}, "go", "build", "-o", out, ".")

	if err != nil {
		return err
	}

	if iconFile, ok := manifest["iconFile"].(string); ok {
		iconBytes, _ := ioutil.ReadFile(iconFile)
		iconBytes64 := base64.StdEncoding.EncodeToString(iconBytes)
		ext := filepath.Ext(iconFile)
		icon64 := fmt.Sprintf("data:image/%s;base64,%s", ext, iconBytes64)
		manifest["icon"] = icon64
	}

	outManifest := filepath.Join(outDir, "manifest.json")

	manifestBytes, _ = json.Marshal(manifest)
	ioutil.WriteFile(outManifest, manifestBytes, 0777)

	return nil
}

func PublishToNavget() error {

	for _, os := range oses {
		if err := buildAndPublish(os); err != nil {
			return err
		}
	}

	return nil
}

func buildAndPublish(os string) error {

	navget, err := build.NewNavgetClient()
	if err != nil {
		return err
	}

	defer sh.Rm("plugin-pub-csv")

	env := map[string]string{
		"GOOS":        os,
		"CGO_ENABLED": "0",
	}

	if err := sh.RunWith(env, "go", "build", "-o", "plugin-pub-csv", "."); err != nil {
		return err
	}

	err = navget.Upload(build.NavgetParams{
		Arch:  "amd64",
		OS:    os,
		Files: []string{"plugin-pub-csv", "icon.png"},
	})

	return err
}

// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("bin")
}

func GenerateGRPC() error {
	destDir := "./internal/pub"
	return plugins.GeneratePublisher(destDir)
}
