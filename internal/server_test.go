package internal_test

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/go-hclog"
	. "github.com/naveego/plugin-pub-csv/internal"
	"github.com/naveego/plugin-pub-csv/internal/pub"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/types"
	"google.golang.org/grpc/metadata"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var timeout = 1 * time.Second


var _ = Describe("Server", func() {

	var (
		sut     pub.PublisherServer
		connect func(dir string, filters ...string) error
	)

	BeforeEach(func() {
		sut = NewServer(hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Trace,
			Output:     GinkgoWriter,
			JSONFormat: true,
		}))
		connect = func(dir string, filters ...string) error {
			var settings Settings
			settings.RootPath = dir
			settings.Filters = filters
			settings.HasHeader = true
			req, _ := pub.NewConnectRequest(settings)
			_, err := sut.Connect(context.Background(), req)
			return err
		}
	})

	Describe("Test connection", func() {

		It("Should error if path is not absolute", func() {
			Expect(connect("bogus")).ToNot(Succeed())
		})

		It("Should error if path is not set", func() {
			Expect(connect("")).ToNot(Succeed())
		})

		It("Should succeed if path is valid", func() {
			Expect(connect(TestDataDir)).To(Succeed())
		})
	})

	Describe("Discover shapes", func() {

		It("Should find files when files are present", func() {
			Expect(connect(TestDataDir, `people\.2\.header\.\w\.csv`)).To(Succeed())
			actual, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
				SampleSize: 5,
				Mode:       pub.DiscoverShapesRequest_ALL,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(actual.Shapes).To(HaveLen(1))
			shape := actual.Shapes[0]
			Expect(shape.Count.Value).To(BeEquivalentTo(6), "should add up counts of all files")

			Expect(shape.Name).To(Equal("people.2.header"))

			Expect(shape.Properties).To(BeEquivalentTo([]*pub.Property{
				{Type: pub.PropertyType_STRING, Id: "first", Name: "first"},
				{Type: pub.PropertyType_STRING, Id: "last", Name: "last"},
				{Type: pub.PropertyType_STRING, Id: "age", Name: "age"},
				{Type: pub.PropertyType_STRING, Id: "date", Name: "date"},
				{Type: pub.PropertyType_STRING, Id: "natural", Name: "natural"},
			}))
		})

		It("Should error when when files have different schemas", func() {
			Expect(connect(TestDataDir, `(people\.3\.header|pets.1.header)\.csv`)).To(Succeed())
			_, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
				SampleSize: 5,
				Mode:       pub.DiscoverShapesRequest_ALL,
			})

			Expect(err).To(MatchError(ContainSubstring("found multiple schemas")))
		})

		It("Should not find files when files are not present", func() {
			Expect(connect(TestDataDir, "missing.file")).To(Succeed())
			actual, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
				SampleSize: 5,
				Mode:       pub.DiscoverShapesRequest_ALL,
			})
			Expect(err).To(BeNil())
			Expect(actual.Shapes).To(BeEmpty())
		})
	})

	Describe("Publish", func() {

		var (
			execute func(settings Settings) ([]*pub.Record, error)
		)

		BeforeEach(func() {
			execute = func(settings Settings) ([]*pub.Record, error) {

				if settings.RootPath == "" {
					settings.RootPath = TestDataDir
				}

				req, _ := pub.NewConnectRequest(settings)
				_, err := sut.Connect(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())

				discoverResponse, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
					SampleSize: 5,
					Mode:       pub.DiscoverShapesRequest_ALL,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(len(discoverResponse.Shapes)).To(BeNumerically(">=", 1))

				shape := discoverResponse.Shapes[0]

				streams := make(chan *publisherStream)

				go func() {
					stream := new(publisherStream)
					stream.err = sut.PublishStream(&pub.PublishRequest{
						Shape: shape,
					}, stream)

					streams <- stream
				}()

				stream := <-streams

				return stream.records, stream.err
			}
		})

		It("Should publish from file with header", func() {
			actual, err := execute(Settings{
				HasHeader: true,
				Filters: []string{"people.3.header.csv",
				}})
			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(HaveLen(3))
			expectedData := []map[string]string{
				{"first": "Ora", "last": "Kennedy", "age": "47", "date": "02/09/1978", "natural": "6252"},
				{"first": "Loretta", "last": "Malone", "age": "41", "date": "06/29/1980", "natural": "1990"},
				{"first": "Jon", "last": "Gray", "age": "35", "date": "12/22/1949", "natural": "4962"},
			}
			var expected []*pub.Record
			for _, m := range expectedData {
				b, _ := json.Marshal(m)
				expected = append(expected, &pub.Record{
					Action:   pub.Record_UPSERT,
					DataJson: string(b),
				})
			}
			Expect(actual).To(BeEquivalentTo(expected))
		})

		It("Should publish from file without header", func() {
			actual, err := execute(Settings{
				HasHeader: false,
				Filters:   []string{"people.3.noheader.csv"},
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(HaveLen(3))
			expectedData := []map[string]string{
				{"Column1": "Ora", "Column2": "Kennedy", "Column3": "47", "Column4": "02/09/1978", "Column5": "6252"},
				{"Column1": "Loretta", "Column2": "Malone", "Column3": "41", "Column4": "06/29/1980", "Column5": "1990"},
				{"Column1": "Jon", "Column2": "Gray", "Column3": "35", "Column4": "12/22/1949", "Column5": "4962"},
			}
			var expected []*pub.Record
			for _, m := range expectedData {
				b, _ := json.Marshal(m)
				expected = append(expected, &pub.Record{
					Action:   pub.Record_UPSERT,
					DataJson: string(b),
				})
			}
			Expect(actual).To(BeEquivalentTo(expected))
		})

		It("Should stop when data is is malformed", func() {
			_, err := execute(Settings{
				HasHeader: false,
				Filters:   []string{"people.7.header.badrecord.csv"},
			})

			Expect(err).To(HaveOccurred())
		})

		Describe("cleanup", func() {
			var (
				root       string
				archiveDir string
				filter     string
				filePath   string
			)
			BeforeEach(func() {
				root, _ = ioutil.TempDir(os.TempDir(), "plugin-pub-csv")
				archiveDir, _ = ioutil.TempDir(os.TempDir(), "plugin-pub-csv-archive")
				filter = "data.csv"
				filePath = filepath.Join(root, filter)
				Expect(ioutil.WriteFile(filePath, []byte(`first,last,age,date,natural
Ora,Kennedy,47,02/09/1978,6252
Loretta,Malone,41,06/29/1980,1990
Jon,Gray,35,12/22/1949,4962
`), 0666)).To(Succeed())
			})
			AfterEach(func() {
				os.RemoveAll(root)
				os.RemoveAll(archiveDir)
			})

			It("should not change file when cleanup is set to nothing", func() {
				actual, err := execute(Settings{
					HasHeader: true,
					RootPath:  root,
					Filters:   []string{filter},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(actual).To(HaveLen(3))
				Expect(filePath).To(BeAnExistingFile())
			})
			It("should delete file when cleanup is set to delete", func() {
				actual, err := execute(Settings{
					HasHeader:     true,
					RootPath:      root,
					Filters:       []string{filter},
					CleanupAction: CleanupDelete,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(actual).To(HaveLen(3))
				Expect(filePath).ToNot(BeAnExistingFile())
			})

			It("should move file when cleanup is set to archive", func() {

				actual, err := execute(Settings{
					HasHeader:     true,
					RootPath:      root,
					Filters:       []string{filter},
					CleanupAction: CleanupArchive,
					ArchivePath:   archiveDir,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(actual).To(HaveLen(3))
				Expect(filePath).ToNot(BeAnExistingFile())
				archivePath := filepath.Join(archiveDir, filter)
				Expect(archivePath).To(BeAnExistingFile())
			})
		})
	})

	Describe("Real Time Publish", func() {

		var (
			execute     func(settings Settings, realTimeSettings RealTimeSettings, realTimeState RealTimeState, shapes ...*pub.Shape) (*publisherStream, error)
			tempDataDir string
		)

		BeforeEach(func() {
			tempDataDir, _ = ioutil.TempDir(os.TempDir(), "plugin-pub-csv")

			execute = func(settings Settings, realTimeSettings RealTimeSettings, realTimeState RealTimeState, shapes ...*pub.Shape) (*publisherStream, error) {

				if settings.RootPath == "" {
					settings.RootPath = tempDataDir
				}

				req, _ := pub.NewConnectRequest(settings)
				_, err := sut.Connect(context.Background(), req)
				Expect(err).ToNot(HaveOccurred())

				var shape *pub.Shape

				if len(shapes) == 0 {
					discoverResponse, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						SampleSize: 5,
						Mode:       pub.DiscoverShapesRequest_ALL,
					})
					Expect(err).ToNot(HaveOccurred())

					shape = discoverResponse.Shapes[0]
				} else {
					discoverResponse, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
						SampleSize: 5,
						Mode:       pub.DiscoverShapesRequest_REFRESH,
						ToRefresh:  shapes,
					})
					Expect(err).ToNot(HaveOccurred())

					shape = discoverResponse.Shapes[0]
				}

				stream := &publisherStream{
					out: make(chan pub.Record, 100),
				}

				go func() {
					defer GinkgoRecover()
					stream.err = sut.PublishStream(&pub.PublishRequest{
						Shape:                shape,
						RealTimeStateJson:    realTimeState.String(),
						RealTimeSettingsJson: realTimeSettings.String(),
					}, stream)
					Expect(stream.err).ToNot(HaveOccurred())
				}()

				return stream, nil
			}
		})

		AfterEach(func() {
			Expect(sut.Disconnect(context.Background(), new(pub.DisconnectRequest))).ToNot(BeNil())
			Expect(os.RemoveAll(tempDataDir)).To(Succeed())
		})

		It("Should commit state when file is completed.", func() {
			fileName := "realtime.1.csv"
			data := []SimpleRecord{
				{ID: "1", Name: "one"},
				{ID: "2", Name: "two"},
			}
			expectedData := makeTempFileSimple(tempDataDir, fileName, data...)

			stream, err := execute(
				Settings{
					HasHeader: true,
					Filters:   []string{fileName},
				},
				RealTimeSettings{
					StartAtDate: time.Time{},
				},
				RealTimeState{},
			)

			Expect(err).ToNot(HaveOccurred())

			Eventually(stream.out).Should(Receive(WithDataJson(expectedData[0])))
			Eventually(stream.out).Should(Receive(WithDataJson(expectedData[1])))

			stat, err := os.Stat(filepath.Join(tempDataDir, fileName))
			expectedState := RealTimeState{
				StartAtDate: stat.ModTime(),
			}
			expectedStateBytes, _ := json.Marshal(expectedState)

			Expect(err).ToNot(HaveOccurred())
			Eventually(stream.out).Should(Receive(And(
				WithAction(pub.Record_REAL_TIME_STATE_COMMIT),
				WithRealTimeStateJson(string(expectedStateBytes)),
			)), "expected real time state commit")
		})

		It("Should publish when new file is written.", func() {

			childDir := filepath.Join(tempDataDir, "child")
			Expect(os.MkdirAll(childDir, 0700)).To(Succeed())

			var expectedData []string

			By("having a file already present", func(){
				data := []SimpleRecord{
					{ID: "1", Name: "one"},
				}
				expectedData = makeTempFileSimple(tempDataDir, "1.csv", data...)
			})

			stream, err := execute(
				Settings{
					HasHeader: true,
					Filters:   []string{".*"},
				},
				RealTimeSettings{
					StartAtDate:            time.Time{},
					ChangeDebounceInterval: 10 * time.Millisecond,
				},
				RealTimeState{},
			)

			By("having a file already present in the root directory")

			Expect(err).ToNot(HaveOccurred())
			Eventually(stream.out, timeout).Should(Receive(WithDataJson(expectedData[0])))
			Eventually(stream.out, timeout).Should(Receive(WithAction(pub.Record_REAL_TIME_STATE_COMMIT)), "expected real time state commit")

			By("having a file added in the root directory", func(){
				data := []SimpleRecord{
					{ID: "2", Name: "root"},
				}
				expectedData = makeTempFileSimple(tempDataDir, "dynamic.csv", data...)
			})
			Eventually(stream.out, timeout).Should(Receive(WithDataJson(expectedData[0])), "expected record from next file")
			Eventually(stream.out, timeout).Should(Receive(WithAction(pub.Record_REAL_TIME_STATE_COMMIT)), "expected real time state commit")

			By("having a file added in the child directory", func(){
				data := []SimpleRecord{
					{ID: "3", Name: "child"},
				}
				expectedData = makeTempFileSimple(childDir, "dynamic-in-child.csv", data...)
			})
			Eventually(stream.out, timeout).Should(Receive(WithDataJson(expectedData[0])), "expected record from next file")
			Eventually(stream.out, timeout).Should(Receive(WithAction(pub.Record_REAL_TIME_STATE_COMMIT)), "expected real time state commit")

			By("having a file added in a newly created child directory", func(){
				childDir := filepath.Join(tempDataDir, "dynamic-child")
				Expect(os.MkdirAll(childDir, 0700)).To(Succeed())
				// allow time for the watcher to subscribe to the new directory
				<-time.After(10*time.Millisecond)
				data := []SimpleRecord{
					{ID: "4", Name: "dynamic-child"},
				}
				expectedData = makeTempFileSimple(childDir, "dynamic-in-dynamic-child.csv", data...)
			})
			Eventually(stream.out, timeout).Should(Receive(WithDataJson(expectedData[0])), "expected record from next file")
			Eventually(stream.out, timeout).Should(Receive(WithAction(pub.Record_REAL_TIME_STATE_COMMIT)), "expected real time state commit")

		})

		It("Should wait until file is calm before publishing.", func() {
			fileNamePrefix := "realtime.detection."
			fileName := fileNamePrefix + "1.csv"
			data := []SimpleRecord{
				{ID: "1", Name: "one"},
			}
			expectedData := makeTempFileSimple(tempDataDir, fileName, data...)

			stream, err := execute(
				Settings{
					HasHeader: true,
					Filters:   []string{"r"},
				},
				RealTimeSettings{
					StartAtDate:            time.Time{},
					ChangeDebounceInterval: 500 * time.Millisecond,
				},
				RealTimeState{},
			)
			Expect(err).ToNot(HaveOccurred())
			Eventually(stream.out).Should(Receive(WithDataJson(expectedData[0])))
			Eventually(stream.out).Should(Receive(WithAction(pub.Record_REAL_TIME_STATE_COMMIT)), "expected real time state commit")

			fileName = fileNamePrefix + "2.csv"
			data = []SimpleRecord{
				{ID: "2", Name: "two"},
			}
			expectedData = makeTempFileSimple(tempDataDir, fileName, data...)

			var record pub.Record
			Eventually(stream.out, 100*time.Millisecond).ShouldNot(Receive(&record), "should wait 500ms before publishing")
			<-time.After(400 * time.Millisecond)

			Eventually(stream.out).Should(Receive(WithDataJson(expectedData[0])))
		})

		It("Should publish even if no files are present yet.", func() {
			fileNamePrefix := "realtime.detection."
			fileName := fileNamePrefix + "1.csv"
			data := []SimpleRecord{
				{ID: "1", Name: "one"},
			}
			stream, err := execute(
				Settings{
					HasHeader: true,
					Filters:   []string{"r"},
				},
				RealTimeSettings{
					StartAtDate:            time.Time{},
					ChangeDebounceInterval: 10 * time.Millisecond,
				},
				RealTimeState{},
				&pub.Shape{
					Id:   "test",
					Name: "test-name",
					Properties: []*pub.Property{
						{Type: pub.PropertyType_STRING, Id: "id", Name: "id"},
						{Type: pub.PropertyType_STRING, Id: "name", Name: "name"},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			<-time.After(100*time.Millisecond)

			expectedData := makeTempFileSimple(tempDataDir, fileName, data...)
			Eventually(stream.out).Should(Receive(WithDataJson(expectedData[0])))
			Eventually(stream.out).Should(Receive(WithAction(pub.Record_REAL_TIME_STATE_COMMIT)), "expected real time state commit")
		})
	})

	Describe("debounce", func() {
		It("should invoke the callback after waiting", func() {
			counter := 0
			sut := Debounce(10*time.Millisecond, func() {
				counter++
			})
			sut()
			sut()
			sut()
			Expect(counter).To(Equal(0))
			<-time.After(15 * time.Millisecond)
			Expect(counter).To(Equal(1))
			sut()
			sut()
			sut()
			Expect(counter).To(Equal(1))
			<-time.After(15 * time.Millisecond)
			Expect(counter).To(Equal(2))
		})
	})
})

func WithDataJson(expected string) GomegaMatcher {
	return WithTransform(func(e pub.Record) string {
		return e.DataJson
	}, Equal(expected))
}

func WithAction(expected pub.Record_Action) GomegaMatcher {
	return WithTransform(func(e pub.Record) pub.Record_Action {
		return e.Action
	}, Equal(expected))
}

func WithRealTimeStateJson(expected string) GomegaMatcher {
	return WithTransform(func(e pub.Record) string {
		return e.RealTimeStateJson
	}, Equal(expected))
}

type publisherStream struct {
	records []*pub.Record
	out     chan pub.Record
	err     error
}

func (p *publisherStream) Send(record *pub.Record) error {
	p.records = append(p.records, record)
	if p.out != nil {
		p.out <- *record
	}
	return nil
}

func (publisherStream) SetHeader(metadata.MD) error {
	panic("implement me")
}

func (publisherStream) SendHeader(metadata.MD) error {
	panic("implement me")
}

func (publisherStream) SetTrailer(metadata.MD) {
	panic("implement me")
}

func (publisherStream) Context() context.Context {
	panic("implement me")
}

func (publisherStream) SendMsg(m interface{}) error {
	panic("implement me")
}

func (publisherStream) RecvMsg(m interface{}) error {
	panic("implement me")
}
