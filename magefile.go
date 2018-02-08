// +build mage

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"

	"github.com/naveego/plugin-pub-csv/version"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var oses = []string{"linux", "darwin", "windows"}

// Default target to run when none is specified
// If not set, running mage will list available targets
// var Default = Build

// A build step that requires additional params, or platform specific steps for example
func Build() error {
	mg.Deps(InstallDeps)
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
	return sh.RunWith(map[string]string{
		"GOOS": os,
	}, "go", "build", "-o", "bin/"+os+"/plugin-pub-csv", ".")
}

func PublishToNavget() error {

	ver := fmt.Sprintf(`"version": "%s",`, version.Version.String())

	manifest, err := replaceInFile("manifest.json", `"version":.*,`, ver)
	if err != nil {
		return err
	}
	defer ioutil.WriteFile("manifest.json", []byte(manifest), 0777)

	for _, os := range oses {
		if err = buildAndPublish(os); err != nil {
			return err
		}
	}

	return nil
}

func buildAndPublish(os string) error {
	defer sh.Rm("plugin-pub-csv")
	defer sh.Rm("package.zip")

	env := map[string]string{
		"GOOS":        os,
		"CGO_ENABLED": "0",
	}

	if err := sh.RunWith(env, "go", "build", "-o", "plugin-pub-csv", "."); err != nil {
		return err
	}

	if err := sh.Run("navget-cli", "publish", "--os", os, "-f", "plugin-pub-csv icon.png"); err != nil {
		return err
	}

	return nil
}

func replaceInFile(file, regex, replacement string) (string, error) {
	input, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile(regex)

	output := re.ReplaceAllString(string(input), replacement)

	err = ioutil.WriteFile(file, []byte(output), 0644)
	return string(input), err
}

// A custom install step if you need your bin someplace other than go/bin
func Install() error {
	mg.Deps(Build)
	fmt.Println("Installing...")
	return os.Rename("./MyApp", "/usr/bin/MyApp")
}

// Manage your deps, or running package managers.
func InstallDeps() error {
	fmt.Println("Installing Deps...")
	cmd := exec.Command("go", "get", "github.com/stretchr/piglatin")
	return cmd.Run()
}

// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("bin")
}
