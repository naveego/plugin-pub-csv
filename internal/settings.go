package internal

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"time"
)

type Settings struct {
	RootPath      string        `json:"rootPath"`
	Filters       []string      `json:filters`
	CleanupAction ArchiveAction `json:archiveAction`
	ArchivePath   string        `json:"archivePath"`
	HasHeader     bool          `json:"hasHeader"`
	Delimiter     string        `json:"delimiter"`
}

// This is the schema which will be returned from the ConfigureRealTime operation.
// language=json
const settingsSchema = `{
  "title": "File Change Detection",
  "type": "object",
  "properties": {
    "startAtDate": {
      "title": "Start At",
      "description": "Minimum created/last modified date for files. Leave blank to include all files.",
      "type": "string",
      "format": "date-time"
    },
	"changeDebounceInterval": {
		"title": "Change Wait Interval",
        "description": "The time to wait before processing a file after it is created or changed. Provide a number and a unit (s = second, m = minute, h = hour).",
		"type": "string",
		"pattern":"\\d+(ms|s|m|h)",
		"default":"5s"
	}
  }
}
`

type RealTimeSettings struct {
	StartAtDate time.Time
	ChangeDebounceInterval time.Duration
}


type realTimeSettingsMarshalling struct {
	StartAtDate time.Time `json:"startAtDate"`
	ChangeDebounceInterval string `json:"changeDebounceInterval"`
}

func (r RealTimeSettings) MarshalJSON() ([]byte, error) {
	m := realTimeSettingsMarshalling{
		StartAtDate:r.StartAtDate,
		ChangeDebounceInterval:r.ChangeDebounceInterval.String(),
	}
	return json.Marshal(m)
}

func (r *RealTimeSettings) UnmarshalJSON(b []byte) error {
	var m realTimeSettingsMarshalling
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	*r = RealTimeSettings{
		StartAtDate:m.StartAtDate,
	}
	r.ChangeDebounceInterval, err = time.ParseDuration(m.ChangeDebounceInterval)
	if err != nil {
		r.ChangeDebounceInterval = 5 * time.Second
	}

	return nil
}

func (r RealTimeSettings) String() string{
	j, _ := json.Marshal(r)
	return string(j)
}

type RealTimeState struct {
	StartAtDate time.Time `json:"startAtDate"`
}

func (r RealTimeState) String() string{
	j, _ := json.Marshal(r)
	return string(j)
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