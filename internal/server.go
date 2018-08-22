package internal

import (
	"os"
	"sync"

	"context"

	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/plugin-pub-csv/internal/pub"
	"github.com/hashicorp/go-hclog"
	"encoding/json"
	"github.com/pkg/errors"
	"path/filepath"
	"regexp"
	"encoding/csv"
	"bytes"
	"io"
	"fmt"
	"strings"
	"math"
)

type Server struct {
	mu             *sync.Mutex
	log            hclog.Logger
	settings       *Settings
	dataPointShape pipeline.Shape
	publishing     bool
	disconnected  chan struct{}
}

func (s *Server) getSettingsAndDisconnector() (*Settings, chan struct{}){
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.settings == nil || s.disconnected == nil {
		return nil, nil
	}
	return s.settings, s.disconnected
}

func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {

	s.disconnect()
	s.disconnected = make(chan struct{})

	s.settings = nil

	settings := new(Settings)
	if err := json.Unmarshal([]byte(req.SettingsJson), settings); err != nil {
		return nil, err
	}

	if err := settings.Validate(); err != nil {
		return nil, err
	}
	s.settings = settings

	return new(pub.ConnectResponse), nil
}

func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverShapesRequest) (*pub.DiscoverShapesResponse, error) {

	files, err := s.findFiles()
	if err != nil {
		return nil, err
	}

	resp := new(pub.DiscoverShapesResponse)

	shapes := make(map[string][]*pub.Shape)

	var summaryShape *pub.Shape

	for _, filePath := range files {
		shape, err := s.buildShapeFromFile(filePath, int(req.SampleSize))
		if err != nil {
			return nil, err
		}

		if summaryShape == nil {
			summaryShape = shape
		} else {
			if summaryShape.Query == shape.Query {
				summaryShape.Count.Value += shape.Count.Value
				summaryShape.Description += ";" + shape.Description
			} else {
				return nil, fmt.Errorf("found multiple schemas: found columns %q in file(s) %q, but columns %q in file %q", summaryShape.Query, summaryShape.Description, shape.Query, shape.Description)
			}
		}

		shapes[shape.Query] = append(shapes[shape.Query], shape)
	}

	if summaryShape != nil {
		resp.Shapes = []*pub.Shape{summaryShape}
	}

	return resp, nil
}

func (s *Server) PublishStream(req *pub.PublishRequest, stream pub.Publisher_PublishStreamServer) error {

	files, err := s.findFiles()
	if err != nil {
		return err
	}

	for _, file := range files {
		err := s.publishRecordsFromFile(file, req.Shape, stream)
		if err != nil {
			return err
		}
		switch s.settings.CleanupAction {
		case CleanupDelete:
			err := os.Remove(file)
			if err != nil {
				return err
			}
		case CleanupArchive:
			rel, err := filepath.Rel(s.settings.RootPath, file)
			if err != nil {
				return fmt.Errorf("could not move file to archiveFilePath: %s",err)
			}
			archiveFilePath := filepath.Join(s.settings.ArchivePath, rel)
			archiveContainer := filepath.Dir(archiveFilePath)
			if err := os.MkdirAll(archiveContainer, 0777); err != nil {
				return fmt.Errorf("could not create archive location for file %q: %s", file, err)
			}
			if err := os.Rename(file, archiveFilePath); err != nil {
				return fmt.Errorf("could not move file %q to archive location: %s", file, err)
			}
		}
	}

	return nil
}

func (s *Server) Disconnect(context.Context, *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {
	s.disconnect()
	return new(pub.DisconnectResponse), nil
}

func (s *Server) disconnect() {

	_, disconnected := s.getSettingsAndDisconnector()
	if disconnected != nil {
		close(disconnected)
	}
}


// NewServer creates a new publisher Server.
func NewServer() pub.PublisherServer {
	return &Server{
		mu: &sync.Mutex{},
		log: hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Trace,
			Output:     os.Stderr,
			JSONFormat: true,
		}),
	}
}

func (s *Server) publishRecordsFromFile(path string, shape *pub.Shape, stream pub.Publisher_PublishStreamServer) (error) {
	var (
		i      int
		row    []string
		record *pub.Record
		err    error
		file   *os.File
	)

	file, err = os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	settings, disconnected := s.getSettingsAndDisconnector()

	if settings.HasHeader {
		_, err = reader.Read()
		if err != nil {
			return err
		}
		i++
	}

	for {
		select{
		case <-disconnected:
			return errNotConnected
		default:
		}

		row, err = reader.Read()
		i++
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		dataMap := make(map[string]string)
		for c := 0; c < len(shape.Properties); c++ {
			dataMap[shape.Properties[c].Id] = row[c]
		}
		record, err = pub.NewRecord(pub.Record_UPSERT, dataMap)
		if err != nil {
			return fmt.Errorf("error creating record at line %d: %s", i, err)
		}

		stream.Send(record)
	}

	return nil
}

func (s *Server) buildShapeFromFile(path string, sampleSize int) (*pub.Shape, error) {

	file, err := os.Open(path)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		return nil, err
	}

	shape := &pub.Shape{
		Description: path,
	}

	reader := csv.NewReader(file)
	reader.Comma = rune(s.settings.Delimiter[0])
	var sample [][]string
	var row []string
	sampleSize += 1
	for i := 0; i < sampleSize; i++ {
		row, err = reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error sampling file %q: %s", path, err)
		}
		sample = append(sample, row)
	}

	if len(sample) == 0 {
		return nil, nil
	}

	hasHeader := s.settings.HasHeader
	row = sample[0]
	var columnIDs []string
	for i, r := range row {
		property := &pub.Property{
			Type: pub.PropertyType_STRING,
		}
		if hasHeader {
			property.Id = r
			property.Name = r
		} else {
			property.Id = fmt.Sprintf("Column%d", i+1)
			property.Name = property.Id
		}
		shape.Properties = append(shape.Properties, property)
		columnIDs = append(columnIDs, property.Id)
	}

	sampleStart := 0
	if hasHeader {
		sampleStart = 1
	}

	for i := sampleStart; i < len(sample); i++ {
		row = sample[i]
		dataMap := make(map[string]string)
		for c := 0; c < len(shape.Properties); c++ {
			dataMap[shape.Properties[c].Id] = row[c]
		}
		var record *pub.Record
		record, err = pub.NewRecord(pub.Record_UPSERT, dataMap)
		if err != nil {
			return nil, fmt.Errorf("error serializing row %d of sample from %q: %s", i, path, err)
		}
		shape.Sample = append(shape.Sample, record)
	}

	shape.Query = strings.Join(columnIDs, "|")

	shape.Count, err = calculateCount(file)
	if hasHeader {
		shape.Count.Value -= 1
	}

	if err != nil {
		return shape, err
	}

	return shape, nil
}

func calculateCount(file *os.File) (*pub.Count, error) {
	file.Seek(0, 0)

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}
	var err error
	c := 0
	out := &pub.Count{
		Kind:pub.Count_EXACT,
		Value:int32(count),
	}

	for {
		c, err = file.Read(buf)
		count += bytes.Count(buf[:c], lineSep)
		if count >= math.MaxInt32 {
			out.Kind = pub.Count_ESTIMATE
			break
		}
		if err == io.EOF {
			// last row doesn't end with \n
			if !bytes.Contains(buf, []byte{'\n',0}) {
				count += 1
			}
		}
		if err != nil {
			break
		}
	}

	out.Value = int32(count)

	if err == io.EOF {
		err = nil
	}

	return out, err
}

func (s *Server) findFiles() ([]string, error) {
	settings, disconnected := s.getSettingsAndDisconnector()

	if settings == nil {
		return nil, errNotConnected
	}

	select{
	case <-disconnected:
		return nil, errNotConnected
	default:
	}

	var files []string

	filepath.Walk(settings.RootPath, func(path string, info os.FileInfo, err error) error {
		select{
		case <-disconnected:
			return errNotConnected
		default:
		}
		var matched = false
		for _, f := range s.settings.Filters {
			if matched, _ = regexp.MatchString(f, path); matched {
				files = append(files, path)
				break
			}
		}
		return nil
	})

	return files, nil
}

var errNotConnected = errors.New("not connected")

