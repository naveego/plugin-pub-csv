package csv_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/fatih/structs"
	"github.com/naveego/api/types/pipeline"
	"github.com/naveego/navigator-go/publishers/protocol"
	. "github.com/naveego/plugin-pub-csv/csv"
)

var _ = Describe("Server", func() {

	var (
		sut      Server
		settings Settings
		csvPath  string
	)

	BeforeEach(func() {
		sut = NewServer()
		csvPath, _ = filepath.Abs("test_data/people.3.header.csv")

		settings = Settings{
			Path:      csvPath,
			HasHeader: true,
		}
	})

	Describe("Test connection", func() {

		It("Should error if path is not absolute", func() {
			settings.Path = "bogus"
			_, err := sut.TestConnection(protocol.TestConnectionRequest{
				Settings: structs.Map(settings),
			})

			Expect(err).ToNot(BeNil())
		})

		It("Should find file when file is present", func() {
			settings.Path = csvPath

			actual, err := sut.TestConnection(protocol.TestConnectionRequest{
				Settings: structs.Map(settings),
			})

			Expect(err).To(BeNil())
			Expect(actual.Success).To(BeTrue())
			Expect(actual.Message).To(ContainSubstring(csvPath))
		})

		It("Should not find file when file is not present", func() {
			settings.Path = csvPath + ".missing"
			actual, err := sut.TestConnection(protocol.TestConnectionRequest{
				Settings: structs.Map(settings),
			})
			Expect(err).To(BeNil())

			Expect(actual.Success).To(BeFalse())
		})

	})

	Describe("Discover shapes", func() {

		It("Should return shapes from file", func() {
			actual, err := sut.DiscoverShapes(protocol.DiscoverShapesRequest{
				Settings: structs.Map(settings),
			})

			Expect(err).To(BeNil())
			Expect(actual.Shapes[0]).To(BeEquivalentTo(
				pipeline.ShapeDefinition{
					Name: csvPath,
					Keys: []string{},
					Properties: []pipeline.PropertyDefinition{
						{Name: "first", Type: "string"},
						{Name: "last", Type: "string"},
						{Name: "age", Type: "string"},
						{Name: "date", Type: "string"},
						{Name: "natural", Type: "string"},
					},
				}))
		})

	})

	Describe("Publish", func() {

		var (
			c       *client
			execute func() []pipeline.DataPoint
		)

		BeforeEach(func() {
			c = newClient()
			execute = func() []pipeline.DataPoint {
				settings.Path = csvPath
				Expect(sut.Init(protocol.InitRequest{
					Settings: structs.Map(settings),
				})).To(BeEquivalentTo(protocol.InitResponse{Success: true}))

				Expect(sut.Publish(protocol.PublishRequest{}, c)).ToNot(BeNil())

				Eventually(c.done).Should(BeClosed())

				Expect(filepath.Glob(filepath.Join(os.TempDir(), "*", "*"+filepath.Base(csvPath)+"*"))).
					To(HaveLen(0), "All copies should be cleaned up.")

				Expect(sut.Dispose(protocol.DisposeRequest{})).To(BeEquivalentTo(protocol.DisposeResponse{Success: true}))

				return c.dataPoints
			}
		})

		It("Should emit data points with data as strings", func() {
			actual := execute()

			Expect(actual).To(HaveLen(3))

			dp := actual[0]
			Expect(dp.Entity).To(Equal(csvPath))
			Expect(dp.Shape).To(BeEquivalentTo(pipeline.Shape{
				KeyNames:   []string{},
				Properties: []string{"first:string", "last:string", "age:string", "date:string", "natural:string"},
			}))

			expected := []map[string]interface{}{
				{"first": "Ora", "last": "Kennedy", "age": "47", "date": "02/09/1978", "natural": "6252"},
				{"first": "Loretta", "last": "Malone", "age": "41", "date": "06/29/1980", "natural": "1990"},
				{"first": "Jon", "last": "Gray", "age": "35", "date": "12/22/1949", "natural": "4962"},
			}

			for i, dp := range actual {
				Expect(dp.Data).To(BeEquivalentTo(expected[i]))
			}
		})

		It("Should handle header correctly", func() {
			csvPath, _ = filepath.Abs("test_data/people.3.noheader.csv")
			settings.HasHeader = false

			actual := execute()

			Expect(actual).To(HaveLen(3))

			dp := actual[0]
			Expect(dp.Action).To(Equal(pipeline.DataPointAction("upsert")))
		})

		It("Should handle glob correctly", func() {
			csvPath, _ = filepath.Abs("test_data/people.2.header.*.csv")

			actual := execute()

			Expect(actual).To(HaveLen(6))
		})

		It("Should emit abend when file isn't a valid csv", func() {
			csvPath, _ = filepath.Abs("test_data/people.nonsense.csv")
			settings.HasHeader = true

			actual := execute()

			Expect(actual).To(HaveLen(1))
			Expect(actual[0].Action).To(Equal(pipeline.DataPointAction("abend")))
		})

		It("Should handle zip correctly", func() {
			csvPath, _ = filepath.Abs("test_data/people.2.header.zip")

			actual := execute()

			Expect(actual).To(HaveLen(6))
		})

	})

})

type client struct {
	dataPoints []pipeline.DataPoint
	done       chan struct{}
}

func newClient() *client {
	return &client{
		done: make(chan struct{}),
	}
}

func (c *client) SendDataPoints(sendRequest protocol.SendDataPointsRequest) (protocol.SendDataPointsResponse, error) {
	c.dataPoints = append(c.dataPoints, sendRequest.DataPoints...)
	return protocol.SendDataPointsResponse{}, nil
}
func (c *client) Done(protocol.DoneRequest) (protocol.DoneResponse, error) {
	close(c.done)
	return protocol.DoneResponse{}, nil
}
