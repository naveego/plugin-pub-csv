package csv

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

type Settings struct {
	Path          string `json:"path"`
	ArchivePath   string `json:"archivePath"`
	HasHeader     bool   `json:"hasHeader"`
	Delimiter     string `json:"delimiter"`
	Shape         string `json:"shape"`
	shapeSettings ShapeSettings
}

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
	if s.Path == "" {
		return errors.New("the Path property must be set")
	}
	if s.Shape == "" {
		return errors.New("the Shape property must be set")
	}

	var shape ShapeSettings
	if err := json.Unmarshal([]byte(s.Shape), &shape); err != nil {
		return fmt.Errorf("couldn't understand shape JSON: %s", err)
	}

	if len(shape.Columns) == 0 {
		return errors.New("shape.columns must have at least one entry")
	}
	if len(shape.Keys) == 0 {
		return errors.New("shape.keys must have at least one entry")
	}

	for i, col := range shape.Columns {

		if col.Name == "" {
			return fmt.Errorf("shape.columns[%d].name is required", i)
		}

		if col.Type == "" {
			return fmt.Errorf("shape.columns[%d].type is required", i)
		}

		if col.Type == "date" && col.Format == "" {
			return fmt.Errorf("shape.columns[%d].format is required because it is a date column", i)
		}
	}

	s.shapeSettings = shape

	return nil
}

func SettingsFromMap(m map[string]interface{}) (Settings, error) {

	var settings Settings
	err := mapstructure.Decode(m, &settings)
	//fmt.Printf("map: %#v\n", m)
	//fmt.Printf("settings: %#v\n", settings)

	if err == nil {
		err = settings.Validate()
	}
	//fmt.Printf("validated settings: %#v\n", settings)

	return settings, err
}
