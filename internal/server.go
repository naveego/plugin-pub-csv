package internal

import (
	"github.com/fsnotify/fsnotify"
	"github.com/vmarkovtsev/go-lcss"
	"os"
	"sort"
	"sync"
	"time"

	"context"

	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/naveego/plugin-pub-csv/internal/pub"
	"github.com/pkg/errors"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"strings"
)

type Server struct {
	mu      *sync.Mutex
	log     hclog.Logger
	session *session
}


// NewServer creates a new publisher Server.
func NewServer(log hclog.Logger) pub.PublisherServer {
	return &Server{
		mu: &sync.Mutex{},
		log: log,
	}
}

func (s *Server) ConnectSession(*pub.ConnectRequest, pub.Publisher_ConnectSessionServer) error {
	panic("implement me")
}

func (s *Server) ConfigureConnection(context.Context, *pub.ConfigureConnectionRequest) (*pub.ConfigureConnectionResponse, error) {
	panic("implement me")
}

func (s *Server) ConfigureQuery(context.Context, *pub.ConfigureQueryRequest) (*pub.ConfigureQueryResponse, error) {
	panic("implement me")
}

func (s *Server) ConfigureRealTime(ctx context.Context, req *pub.ConfigureRealTimeRequest) (*pub.ConfigureRealTimeResponse, error) {
	if req.Form == nil {
		req.Form = &pub.ConfigurationFormRequest{}
	}

	resp := &pub.ConfigureRealTimeResponse{
		Form: &pub.ConfigurationFormResponse{
			DataJson:   req.Form.DataJson,
			SchemaJson: settingsSchema,
		},
	}

	return resp, nil
}

func (s *Server) BeginOAuthFlow(context.Context, *pub.BeginOAuthFlowRequest) (*pub.BeginOAuthFlowResponse, error) {
	panic("implement me")
}

func (s *Server) CompleteOAuthFlow(context.Context, *pub.CompleteOAuthFlowRequest) (*pub.CompleteOAuthFlowResponse, error) {
	panic("implement me")
}

type session struct {
	settings         Settings
	realTimeSettings *RealTimeSettings
	realTimeState    *RealTimeState
	ctx              context.Context
	cancel           func()
	// error set when session.fail(err) is called with an error
	err error
}

func (s *session) fail(err error) {
	s.err = err
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Server) Connect(ctx context.Context, req *pub.ConnectRequest) (*pub.ConnectResponse, error) {

	s.disconnect()
	s.session = &session{}
	s.session.ctx, s.session.cancel = context.WithCancel(context.Background())

	settings := Settings{}
	if err := json.Unmarshal([]byte(req.SettingsJson), &settings); err != nil {
		return nil, err
	}

	if err := settings.Validate(); err != nil {
		return nil, err
	}

	s.session.settings = settings

	return new(pub.ConnectResponse), nil
}

func (s *Server) DiscoverShapes(ctx context.Context, req *pub.DiscoverShapesRequest) (*pub.DiscoverShapesResponse, error) {

	session, err := s.getSession()
	if err != nil {
		return nil, err
	}

	files, err := s.findFiles(session)
	if err != nil {
		return nil, err
	}

	resp := new(pub.DiscoverShapesResponse)

	shapes := make(map[string][]*pub.Shape)

	var summaryShape *pub.Shape

	var nameComponents [][]byte

	for filePath := range files {
		nameComponents = append(nameComponents, []byte(filePath))

		shape, err := s.buildShapeFromFile(session, filePath, int(req.SampleSize))
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
		nameBytes := lcss.LongestCommonSubstring(nameComponents...)
		bestName := filepath.Base(string(nameBytes))
		summaryShape.Name = strings.Trim(bestName, "-._")
		resp.Shapes = []*pub.Shape{summaryShape}
	}

	if req.Mode == pub.DiscoverShapesRequest_REFRESH {
		// if we're being asked to refresh, but we didn't find any files,
		// we'll assume that files might show up in the future and just return
		// the shape we were supposed to refresh
		if len(resp.Shapes) == 0 {
			resp.Shapes = req.ToRefresh
		}
	}

	return resp, nil
}

func (s *Server) PublishStream(req *pub.PublishRequest, stream pub.Publisher_PublishStreamServer) error {
	var err error
	session := s.session

	if req.RealTimeSettingsJson != "" {
		err = json.Unmarshal([]byte(req.RealTimeSettingsJson), &session.realTimeSettings)
		if err != nil {
			return errors.Wrap(err, "real time settings invalid")
		}

		if req.RealTimeStateJson == "" {
			session.realTimeState = new(RealTimeState)
		} else {
			err = json.Unmarshal([]byte(req.RealTimeStateJson), &session.realTimeState)
			if err != nil {
				return errors.Wrap(err, "real time state invalid")
			}
		}

		if session.realTimeState.StartAtDate.IsZero() {
			// state did not have a commit, so set it from the real time settings
			session.realTimeState.StartAtDate = session.realTimeSettings.StartAtDate
		}
	}

	files, err := s.findFiles(session)
	if err != nil {
		return err
	}

	settings := session.settings
	isRealTime := session.realTimeState != nil

	for file := range files {

		// stop processing if session is canceled
		select {
		case <-session.ctx.Done():
			return session.err
		default:
		}

		err := s.publishRecordsFromFile(session, file, req.Shape, stream)
		if err != nil {
			return err
		}

		if isRealTime {
			s.log.Info("Committing real time state for file.", "path", file)
			info, err := os.Stat(file)
			if err != nil {
				return err
			}
			session.realTimeState.StartAtDate = info.ModTime()
			err = stream.Send(&pub.Record{
				Action:            pub.Record_REAL_TIME_STATE_COMMIT,
				RealTimeStateJson: session.realTimeState.String(),
			})
			if err != nil {
				return errors.Errorf("error committing state %q: %s", session.realTimeState.String(), err)
			}
		}

		switch settings.CleanupAction {
		case CleanupDelete:
			err := os.Remove(file)
			if err != nil {
				return err
			}
		case CleanupArchive:
			rel, err := filepath.Rel(settings.RootPath, file)
			if err != nil {
				return fmt.Errorf("could not move file to archiveFilePath: %s", err)
			}
			archiveFilePath := filepath.Join(settings.ArchivePath, rel)
			archiveContainer := filepath.Dir(archiveFilePath)
			if err := os.MkdirAll(archiveContainer, 0777); err != nil {
				return fmt.Errorf("could not create archive location for file %q: %s", file, err)
			}
			if err := os.Rename(file, archiveFilePath); err != nil {
				return fmt.Errorf("could not move file %q to archive location: %s", file, err)
			}
		}
	}

	return session.err
}

func (s *Server) Disconnect(context.Context, *pub.DisconnectRequest) (*pub.DisconnectResponse, error) {
	s.disconnect()
	return new(pub.DisconnectResponse), nil
}

func (s *Server) disconnect() {
	session, err := s.getSession()
	if err != nil {
		return
	}

	s.session = nil
	if session != nil && session.cancel != nil {
		s.log.Info("Terminating previous session.")
		session.cancel()
	}
}

func (s *Server) getSession() (*session, error) {
	session := s.session
	if session == nil {
		return nil, errors.New("no session (connect not called)")
	}

	if session.ctx.Err() != nil {
		return nil, errors.New("no session (previous session disconnected)")
	}

	return session, nil
}

func (s *Server) publishRecordsFromFile(session *session, path string, shape *pub.Shape, stream pub.Publisher_PublishStreamServer) error {
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
	settings := session.settings

	if settings.HasHeader {
		_, err = reader.Read()
		if err != nil {
			return err
		}
		i++
	}

	for {
		select {
		case <-session.ctx.Done():
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

		err = stream.Send(record)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) buildShapeFromFile(session *session, path string, sampleSize int) (*pub.Shape, error) {

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

	settings := session.settings
	reader := csv.NewReader(file)
	reader.Comma = rune(settings.Delimiter[0])
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

	hasHeader := settings.HasHeader
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
	_, err := file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	c := 0
	out := &pub.Count{
		Kind:  pub.Count_EXACT,
		Value: int32(count),
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
			if !bytes.Contains(buf, []byte{'\n', 0}) {
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

func (s *Server) findFiles(session *session) (chan string, error) {

	select {
	case <-session.ctx.Done():
		return nil, errNotConnected
	default:
	}

	settings := session.settings

	// default to no min mod time
	minModTime := time.Time{}
	if session.realTimeSettings != nil {
		// if there are settings, get the min mod time from that
		minModTime = session.realTimeSettings.StartAtDate
	}
	if session.realTimeState != nil && minModTime.Before(session.realTimeState.StartAtDate) {
		// if there is an existing time from a committed state, use that:
		minModTime = session.realTimeState.StartAtDate
	}

	filter := func(path string, info os.FileInfo) bool {
		if info == nil {
			return false
		}
		if info.IsDir() {
			return false
		}
		var matched = false
		for _, f := range settings.Filters {
			if matched, _ = regexp.MatchString(f, path); matched {
				break
			}
		}
		if matched {
			// file path matches filters, make sure that the file is new enough
			matched = info.ModTime().After(minModTime)
		}
		return matched
	}

	var initialFiles []queuedFile

	err := filepath.Walk(settings.RootPath, func(path string, info os.FileInfo, err error) error {
		select {
		case <-session.ctx.Done():
			return errNotConnected
		default:
			if filter(path, info) {
				initialFiles = append(initialFiles, queuedFile{path, info})
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// make sure files are sorted by mod time so that commits will be in the right order
	sort.Sort(filesOrderedByModTime(initialFiles))

	out := make(chan string, len(initialFiles))
	for _, f := range initialFiles {
		out <- f.path
	}

	if session.realTimeSettings == nil {
		// no file watcher will be available, so we're done here
		close(out)
	} else {
		// this is a real time publish, so we need to keep watching for files
		go func() {

			defer close(out)

			e := s.watchFiles(session, filter, out)
			if e != nil {
				e = errors.Wrap(e, "watch files failed")
				session.fail(e)
			}
		}()
	}

	return out, nil
}

func (s *Server) watchFiles(session *session, filter func(path string, info os.FileInfo) bool, outCh chan string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "create file watcher")
	}

	s.log.Debug("created file watcher")

	err = watcher.Add(session.settings.RootPath)
	if err != nil {
		return errors.Wrap(err, "watch root path")
	}
	s.log.Info("watching root path", "path", session.settings.RootPath)

	err = filepath.Walk(session.settings.RootPath, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			err = watcher.Add(path)
			if err != nil {
				return err
			}
			s.log.Info("watching child path", "path", path)
		}
		return nil
	})

	if err != nil {
		return errors.Wrap(err, "watch child path")
	}

	defer watcher.Close()

	debouncers := map[string]func(){}

	for {
		select {
		case <-session.ctx.Done():
			return nil

		case event, ok := <-watcher.Events:
			s.log.Debug("got file event", "event", event.String())

			if !ok {
				return nil
			}
			name := event.Name
			isCreate := event.Op&fsnotify.Create == fsnotify.Create
			isWrite := event.Op&fsnotify.Write == fsnotify.Write
			isDelete := event.Op&fsnotify.Remove == fsnotify.Remove
			if isDelete {
				continue
			}

			info, err := os.Stat(name)
			if err != nil {
				return err
			}
			if info.IsDir() {
				if isCreate {
					s.log.Info("New directory added, will be watched.", "path", name)
					err = watcher.Add(name)
					if err != nil {
						return errors.Wrap(err, "watching new directory")
					}
				}
			} else {
				if !filter(name, info) {
					continue
				}

				// When a matching file is written or created, we'll
				// wait for an interval before telling the publisher to
				// start reading it.
				if isCreate || isWrite {
					s.log.Debug("File change detected.", "path", name)
					fn, ok := debouncers[name]
					if !ok {
						fn = Debounce(session.realTimeSettings.ChangeDebounceInterval, func() {
							s.log.Debug("File change debounce expired.", "path", name)
							outCh <- name
						})
						debouncers[name] = fn
					}
					fn()
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			s.log.Warn("File watcher had error.", "error", err)
		}
	}
}

// Creates a debounced function that delays invoking fn until after wait milliseconds have elapsed since the last time the debounced function was invoked.
func Debounce(wait time.Duration, fn func()) func() {
	t := time.NewTimer(wait)
	want := make(chan struct{}, 1)

	go func() {
		for {
			<-want
			<-t.C
			fn()
		}
	}()

	return func() {
		t.Stop()
		t.Reset(wait)
		select {
		case want <- struct{}{}:
		default:
		}
	}
}

type queuedFile struct {
	path string
	info os.FileInfo
}

type filesOrderedByModTime []queuedFile

func (q filesOrderedByModTime) Len() int           { return len(q) }
func (q filesOrderedByModTime) Less(i, j int) bool { return q[i].info.ModTime().Before(q[j].info.ModTime()) }
func (q filesOrderedByModTime) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

var errNotConnected = errors.New("not connected")
