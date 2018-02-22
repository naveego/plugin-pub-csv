package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/navigator-go/publishers/protocol"
)

type server struct {
	mu          *sync.Mutex
	settings    Settings
	publishing  bool
	stopPublish func()
}

type Server interface {
	protocol.DataPublisher
	protocol.ShapeDiscoverer
	protocol.ConnectionTester
}

// Newserver creates a new MSSQL publisher instance
func NewServer() Server {
	return &server{
		mu: &sync.Mutex{},
	}
}

func (m *server) Init(request protocol.InitRequest) (protocol.InitResponse, error) {
	settings, err := SettingsFromMap(request.Settings)
	if err != nil {
		return protocol.InitResponse{}, err
	}
	m.settings = settings
	return protocol.InitResponse{Success: true}, nil
}

func (m *server) Dispose(protocol.DisposeRequest) (protocol.DisposeResponse, error) {

	return protocol.DisposeResponse{Success: true}, nil

}

func (m *server) Publish(request protocol.PublishRequest, toClient protocol.PublisherClient) (protocol.PublishResponse, error) {

	response := protocol.PublishResponse{}
	files, err := getAllMatchingFiles(m.settings.Path)
	if err != nil {
		return response, err
	}

	tmpdir, filesToProcess, err := prepareFilesForProcessing(m.settings.Path)
	if err != nil {
		return response, err
	}

	go func() {
		defer func() {
			toClient.Done(protocol.DoneRequest{})
			if err := os.RemoveAll(tmpdir); err != nil {
				logrus.WithField("tmpdir", tmpdir).WithError(err).Error("Couldn't delete temporary directory.")
			}

			m.publishing = false
		}()

		for _, file := range filesToProcess {
			log := logrus.WithField("file", unmanglePath(file))
			log.Info("Processing file.")
			if err := m.processFile(toClient, m.settings.Path, file, log); err != nil {
				log.WithError(err).Error("Error while processing file.")
				dp := pipeline.DataPoint{
					Action: "abend",
					Entity: m.settings.Path,
					Meta: map[string]string{
						"csv:error": err.Error(),
						"csv:file":  unmanglePath(file),
					},
				}

				toClient.SendDataPoints(protocol.SendDataPointsRequest{DataPoints: []pipeline.DataPoint{dp}})
			}
		}

	}()

	response.Success = true
	response.Message = "Publishing file(s): " + strings.Join(files, ";")

	return response, nil

}

func (m *server) streamRecordsFromFile(filePath string, limit int, records chan []string, errors chan error) {

	defer close(records)
	defer close(errors)

	file, err := os.Open(filePath)
	if err != nil {
		errors <- fmt.Errorf("couldn't open file: %s", err)
		return
	}

	defer file.Close()

	reader := csv.NewReader(file)

	for count := 0; count < limit; count++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors <- fmt.Errorf("error reading from file: %s", err)
			return
		}

		records <- record
	}
}

func (m *server) processFile(c protocol.PublisherClient, entityName string, filePath string, log *logrus.Entry) error {

	failures := make(chan error, 1)
	records := make(chan []string)

	go m.streamRecordsFromFile(filePath, math.MaxInt64, records, failures)

	var cols []string
	var props []string

	count := 0
	for {
		select {
		case err := <-failures:
			if err != nil {
				return err
			}
		case record, more := <-records:
			if !more {
				return nil
			}

			if cols == nil {
				if m.settings.HasHeader {
					cols = record
					for _, c := range cols {
						props = append(props, c+":string")
					}
					continue
				}
				for i := range record {
					cols = append(cols, fmt.Sprintf("Column%d", i))
					props = append(props, fmt.Sprintf("Column%d:string", i))
				}
			}

			dp := pipeline.DataPoint{
				Action: "upsert",
				Shape: pipeline.Shape{
					KeyNames:   []string{},
					Properties: props,
				},
				Entity: entityName,
				Data:   make(map[string]interface{}),
			}

			for i, v := range record {
				col := cols[i]
				dp.Data[col] = v
			}

			c.SendDataPoints(protocol.SendDataPointsRequest{
				DataPoints: []pipeline.DataPoint{dp},
			})

			count++
			if count%100 == 0 {
				log.WithField("count", count).Debug("Processing file...")
			}
		}
	}
}

func (m *server) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	response := protocol.DiscoverShapesResponse{}

	settings, err := SettingsFromMap(request.Settings)
	if err != nil {
		return response, err
	}

	filesToProcess, err := getAllMatchingFiles(settings.Path)
	if err != nil {
		return response, err
	}
	if len(filesToProcess) == 0 {
		return response, errors.New("no files found")
	}

	var samples [][]string

	failures := make(chan error, 1)
	records := make(chan []string)
	filePath := filesToProcess[0]

	go m.streamRecordsFromFile(filePath, 10, records, failures)

	for record := range records {
		samples = append(samples, record)
	}

	err = <-failures
	if err != nil {
		return response, fmt.Errorf("attempt to examine file result in error: %s", err)
	}

	if len(samples) == 0 {
		return response, fmt.Errorf("examined file %q but found no rows", filePath)
	}

	sd := pipeline.ShapeDefinition{
		Name: settings.Path,
		Keys: []string{},
	}

	var cols []string

	if settings.HasHeader {
		cols = samples[0]
		samples = samples[1:]
	} else {
		for i := range samples[0] {
			cols = append(cols, fmt.Sprintf("Column%d", i))
		}
	}

	for _, col := range cols {
		sd.Properties = append(sd.Properties, pipeline.PropertyDefinition{
			Name: col,
			Type: "string",
		})
	}

	response.Shapes = []pipeline.ShapeDefinition{sd}
	return response, nil
}

func (m *server) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {

	response := protocol.TestConnectionResponse{}

	settings, err := SettingsFromMap(request.Settings)
	if err != nil {
		return response, fmt.Errorf("couldn't decode settings: %s", err)
	}

	paths, err := getAllMatchingFiles(settings.Path)

	response.Success = len(paths) > 0 && err == nil

	if err != nil {
		response.Message = fmt.Sprintf("Couldn't resolve path: %s", err)
	} else if len(paths) == 0 {
		response.Message = fmt.Sprintf("No paths found matching %s", settings.Path)
	} else {
		response.Message = "Paths: " + strings.Join(paths, ";")
	}

	return response, err

}
