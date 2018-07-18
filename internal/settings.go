package internal

import (
	"errors"
	"path/filepath"
)

type Settings struct {
	RootPath      string        `json:"rootPath"`
	Filters       []string      `json:filters`
	CleanupAction ArchiveAction `json:archiveAction`
	ArchivePath   string        `json:"archivePath"`
	HasHeader     bool          `json:"hasHeader"`
	Delimiter     string        `json:"delimiter"`
}

type ArchiveAction string
const (
	CleanupNothing = ArchiveAction("nothing")
	CleanupArchive = ArchiveAction("archive")
	CleanupDelete  = ArchiveAction("delete")
)

type ShapeSettings struct {
	Name    string
	Columns []ShapeColumn
	Keys    []string
}

type ShapeColumn struct {
	Name   string
	Type   string
	Format string
}

// Validate returns an error if the Settings are not valid.
// It also populates the internal fields of settings.
func (s *Settings) Validate() error {
	if s.RootPath == "" {
		return errors.New("the rootPath property must be set")
	}

	if !filepath.IsAbs(s.RootPath) {
		return errors.New("the rootPath property must be an absolute path")
	}

	if s.CleanupAction == "" {
		s.CleanupAction = CleanupNothing
	}

	if len(s.Filters) == 0 {
		s.Filters = []string{`\.csv$`}
	}

	if s.Delimiter == "" {
		s.Delimiter = ","
	}

	return nil
}