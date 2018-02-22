package csv

import (
	"errors"

	"github.com/mitchellh/mapstructure"
)

type Settings struct {
	Path      string `json:"path"`
	HasHeader bool   `json:"hasHeader"`
	Delimiter string `json:"delimiter"`
}

// Validate returns an error if the Settings are not valid.
// It also populates the internal fields of settings.
func (s *Settings) Validate() error {
	if s.Path == "" {
		return errors.New("the Path property must be set")
	}
	if s.Delimiter == "" {
		s.Delimiter = ","
	}

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
