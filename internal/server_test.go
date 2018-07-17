package internal_test

import (
	"os"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/naveego/plugin-pub-csv/internal"
	"github.com/naveego/plugin-pub-csv/internal/pub"
	"context"
	"google.golang.org/grpc/metadata"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

var _ = Describe("Server", func() {

	var (
		sut      pub.PublisherServer
		rootPath string
		connect  func(rootPath string, filters ...string) error
	)

	BeforeEach(func() {
		sut = NewServer()
		rootPath = os.ExpandEnv("$GOPATH/src/github.com/naveego/plugin-pub-csv/test/test_data")
		// shape, _ := json.Marshal(ShapeSettings{
		// 	Name: "TestShape",
		// 	Keys: []string{"first", "natural"},
		// 	Columns: []ShapeColumn{
		// 		{Name: "first", Type: "string"},
		// 		{Name: "last", Type: "string"},
		// 		{Name: "age", Type: "number"},
		// 		{Name: "date", Type: "date", Format: "MM/dd/yyyy"},
		// 		{Name: "natural", Type: "number"},
		// 	},
		// })
		connect = func(rootPath string, filters ...string) error {
			var settings Settings
			settings.RootPath = rootPath
			settings.Filters = filters
			settings.HasHeader = true
			req, _ := pub.NewConnectRequest(settings)
			_, err := sut.Connect(context.Background(), req)
			return err
		}
	})

	Describe("Validate settings", func() {

		It("Should error if path is not absolute", func() {
			settings := &Settings{RootPath:"bogus"}
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if path is not set", func() {
			settings := &Settings{}
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should set defaults", func() {
			settings := &Settings{RootPath:"/"}
			Expect(settings.Validate()).To(Succeed())
			Expect(settings.Filters).To(BeEquivalentTo([]string{`\.csv$`}))
			Expect(settings.Delimiter).To(BeEquivalentTo(","))
			Expect(settings.CleanupAction).To(Equal(CleanupNothing))
		})
	})

	Describe("Test connection", func() {

		It("Should error if path is not absolute", func() {
			Expect(connect("bogus")).ToNot(Succeed())
		})

		It("Should error if path is not set", func() {
			Expect(connect("")).ToNot(Succeed())
		})

		It("Should succeed if path is valid", func() {
			Expect(connect(rootPath)).To(Succeed())
		})
	})

	Describe("Discover shapes", func() {

		It("Should find files when files are present", func() {
			Expect(connect(rootPath, `people\.2\.header\.\w\.csv`)).To(Succeed())
			actual, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
				SampleSize: 5,
				Mode:       pub.DiscoverShapesRequest_ALL,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(actual.Shapes).To(HaveLen(1))
			shape := actual.Shapes[0]
			Expect(shape.Count.Value).To(BeEquivalentTo(6), "should add up counts of all files")

			Expect(shape.Properties).To(BeEquivalentTo([]*pub.Property{
				{Type: pub.PropertyType_STRING, Id: "first", Name: "first"},
				{Type: pub.PropertyType_STRING, Id: "last", Name: "last"},
				{Type: pub.PropertyType_STRING, Id: "age", Name: "age"},
				{Type: pub.PropertyType_STRING, Id: "date", Name: "date"},
				{Type: pub.PropertyType_STRING, Id: "natural", Name: "natural"},
			}))
		})

		It("Should error when when files have different schemas", func() {
			Expect(connect(rootPath, `(people\.3\.header|pets.1.header)\.csv`)).To(Succeed())
			_, err := sut.DiscoverShapes(context.Background(), &pub.DiscoverShapesRequest{
				SampleSize: 5,
				Mode:       pub.DiscoverShapesRequest_ALL,
			})

			Expect(err).To(MatchError(ContainSubstring("found multiple schemas")))
		})

		It("Should not find files when files are not present", func() {
			Expect(connect(rootPath, "missing.file")).To(Succeed())
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
					settings.RootPath = rootPath
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
				HasHeader:true,
				Filters:[]string{"people.3.header.csv",
			}})
			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(HaveLen(3))
			expectedData := []map[string]string {
				{"first":"Ora","last":"Kennedy","age":"47","date":"02/09/1978","natural":"6252"},
				{"first":"Loretta","last":"Malone","age":"41","date":"06/29/1980","natural":"1990"},
				{"first":"Jon","last":"Gray","age":"35","date":"12/22/1949","natural":"4962"},
			}
			var expected []*pub.Record
			for _, m := range expectedData {
				b, _ := json.Marshal(m)
				expected = append(expected, &pub.Record{
					Action:pub.Record_UPSERT,
					DataJson: string(b),
				})
			}
			Expect(actual).To(BeEquivalentTo(expected))
		})

		It("Should publish from file without header", func() {
			actual, err := execute(Settings{
				HasHeader:false,
				Filters:[]string{"people.3.noheader.csv"},
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(HaveLen(3))
			expectedData := []map[string]string {
				{"Column1":"Ora","Column2":"Kennedy","Column3":"47","Column4":"02/09/1978","Column5":"6252"},
				{"Column1":"Loretta","Column2":"Malone","Column3":"41","Column4":"06/29/1980","Column5":"1990"},
				{"Column1":"Jon","Column2":"Gray","Column3":"35","Column4":"12/22/1949","Column5":"4962"},
			}
			var expected []*pub.Record
			for _, m := range expectedData {
				b, _ := json.Marshal(m)
				expected = append(expected, &pub.Record{
					Action:pub.Record_UPSERT,
					DataJson: string(b),
				})
			}
			Expect(actual).To(BeEquivalentTo(expected))
		})


		It("Should stop when data is is malformed", func() {
			_, err := execute(Settings{
				HasHeader:false,
				Filters:[]string{"people.7.header.badrecord.csv"},
			})

			Expect(err).To(HaveOccurred())
		})

		Describe("cleanup", func(){
			var (
				root       string
				archiveDir string
				filter     string
				filePath   string
			)
			BeforeEach(func(){
				root, _ = ioutil.TempDir(os.TempDir(),"plugin-pub-csv")
				archiveDir, _ = ioutil.TempDir(os.TempDir(),"plugin-pub-csv-archive")
				filter = "data.csv"
				filePath = filepath.Join(root, filter)
				ioutil.WriteFile(filePath, []byte(`first,last,age,date,natural
Ora,Kennedy,47,02/09/1978,6252
Loretta,Malone,41,06/29/1980,1990
Jon,Gray,35,12/22/1949,4962
`), 0666)
			})
			AfterEach(func(){
				os.RemoveAll(root)
				os.RemoveAll(archiveDir)
			})

			It("should not change file when cleanup is set to nothing", func(){
				actual, err := execute(Settings{
					HasHeader:true,
					RootPath: root,
					Filters:[]string{filter},
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(actual).To(HaveLen(3))
				Expect(filePath).To(BeAnExistingFile())
			})
			It("should delete file when cleanup is set to delete", func(){
				actual, err := execute(Settings{
					HasHeader:true,
					RootPath: root,
					Filters:[]string{filter},
					CleanupAction:CleanupDelete,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(actual).To(HaveLen(3))
				Expect(filePath).ToNot(BeAnExistingFile())
			})

			It("should move file when cleanup is set to archive", func(){

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

})

type publisherStream struct {
	records []*pub.Record
	err     error
}

func (p *publisherStream) Send(record *pub.Record) error {
	p.records = append(p.records, record)
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
