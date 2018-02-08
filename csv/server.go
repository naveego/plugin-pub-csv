package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/vjeantet/jodaTime"

	"github.com/sirupsen/logrus"

	"github.com/satori/go.uuid"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/navigator-go/publishers/protocol"
)

type server struct {
	mu             *sync.Mutex
	settings       Settings
	dataPointShape pipeline.Shape
	publishing     bool
	stopPublish    func()
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
	m.dataPointShape = pipeline.Shape{
		KeyNames: m.settings.shapeSettings.Keys,
	}
	for _, p := range m.settings.shapeSettings.Columns {
		m.dataPointShape.Properties = append(m.dataPointShape.Properties, fmt.Sprintf("%s:%s", p.Name, p.Type))
	}

	return protocol.InitResponse{Success: true}, nil

}

func (m *server) Dispose(protocol.DisposeRequest) (protocol.DisposeResponse, error) {

	return protocol.DisposeResponse{Success: true}, nil

}

func (m *server) Publish(request protocol.PublishRequest, toClient protocol.PublisherClient) (protocol.PublishResponse, error) {

	response := protocol.PublishResponse{}
	files, err := getResolvedFilePaths(m.settings.Path)
	if err != nil {
		return response, err
	}

	u, _ := uuid.NewV4()
	var tmpdir = filepath.Join(os.TempDir(), u.String())
	if err := os.Mkdir(tmpdir, 0777); err != nil {
		return response, fmt.Errorf("couldn't create temp directory %s: %s", tmpdir, err)
	}

	var filesToProcess []string

	for _, file := range files {
		if strings.HasSuffix(file, ".zip") {
			return response, fmt.Errorf("can't handle file %s, zip support not implemented yet", file)
		}
		u, _ = uuid.NewV4()
		fileName := fmt.Sprintf("%s.%s", filepath.Base(file), u.String())
		fileToProcess := filepath.Join(tmpdir, fileName)

		if err := copyFileContents(file, fileToProcess); err != nil {
			return response, fmt.Errorf("couldn't copy file %s to temp directory for processing: %s", fileToProcess, err)
		}

		filesToProcess = append(filesToProcess, fileToProcess)
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
			log := logrus.WithField("file", file)
			log.Info("Processing file.")
			if err := m.processFile(toClient, file, log); err != nil {
				log.WithError(err).Error("Error while processing file.")
				dp := pipeline.DataPoint{
					Action: "abend",
					Shape:  m.dataPointShape,
					Entity: m.settings.shapeSettings.Name,
					Meta: map[string]string{
						"csv:error": err.Error(),
						"csv:file":  file,
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

func (m *server) processFile(c protocol.PublisherClient, filePath string, log *logrus.Entry) error {

	cols := m.settings.shapeSettings.Columns

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("couldn't open file: %s", err)
	}

	defer file.Close()

	reader := csv.NewReader(file)
	if m.settings.HasHeader {
		// discard the header row
		if _, err := reader.Read(); err != nil {
			return fmt.Errorf("error reading header: %s", err)
		}
	}
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading from file: %s", err)
		}
		if len(record) != len(cols) {
			return fmt.Errorf("record in file %s had %d columns, but only %d columns are defined in shape",
				filePath, len(record), len(cols))
		}

		dp := pipeline.DataPoint{
			Action: "upsert",
			Shape:  m.dataPointShape,
			Entity: m.settings.shapeSettings.Name,
			Data:   make(map[string]interface{}),
		}

		for i, v := range record {
			col := cols[i]
			switch col.Type {
			case "number":
				if dp.Data[col.Name], err = strconv.Atoi(v); err != nil {
					dp.Action = "malformed"
					dp.Data[col.Name] = fmt.Sprintf("could not parse '%s' as number: %s", v, err)
				}
			case "date":
				if dp.Data[col.Name], err = jodaTime.Parse(col.Format, v); err != nil {
					dp.Action = "malformed"
					dp.Data[col.Name] = fmt.Sprintf("could not parse '%s' as date using format '%s': %s", v, col.Format, err)
				}
			default:
				dp.Data[col.Name] = v
			}
		}

		c.SendDataPoints(protocol.SendDataPointsRequest{
			DataPoints: []pipeline.DataPoint{dp},
		})

		count++
		if count%100 == 0 {
			log.WithField("count", count).Debug("Processing file...")
		}
	}

	return nil
}

func (m *server) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	response := protocol.DiscoverShapesResponse{}

	settings, err := SettingsFromMap(request.Settings)
	if err != nil {
		return response, err
	}

	sd := pipeline.ShapeDefinition{
		Name: settings.shapeSettings.Name,
		Keys: settings.shapeSettings.Keys,
	}
	for _, col := range settings.shapeSettings.Columns {
		sd.Properties = append(sd.Properties, pipeline.PropertyDefinition{
			Name: col.Name,
			Type: col.Type,
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

	paths, err := getResolvedFilePaths(settings.Path)

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

func getResolvedFilePaths(path string) ([]string, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%s is not an absolute path", path)
	}

	files, err := filepath.Glob(path)
	return files, err
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}
