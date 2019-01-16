package internal_test

import (
	"encoding/csv"
	"encoding/json"
	"github.com/hashicorp/go-hclog"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCsv(t *testing.T) {
	RegisterFailHandler(Fail)

	hclog.DefaultOptions.Output = GinkgoWriter

	RunSpecs(t, "Csv Suite")
}

var TestDataDir string

var _ = BeforeSuite(func(){
	_, thisPath, _, _ := runtime.Caller(0)
	packagePath := filepath.Dir(filepath.Dir(thisPath))
	TestDataDir = filepath.Join(packagePath, "test/test_data")
})

var _ = Describe("channels, how do they work", func(){
	It("closing a channel should not discard buffered items", func(){
		inCh := make(chan int, 5)
		for i := 0; i<5; i++  {
			inCh <-i
		}
		close(inCh)
		outCh := make(chan int)
		go func(){
			for i := range inCh{
				outCh<-i
			}
			close(outCh)
		}()

		Eventually(outCh).Should(Receive(Equal(4)))

	})
})



type SimpleRecord struct {
	ID string `json:"id"`
	Name string `json:"name"`
}

func makeTempFileSimple(dir, name string, in ...SimpleRecord) (expected []string) {
	records := [][]string{
		{"id", "name"},
	}

	for _, i := range in {
		records = append(records, []string{i.ID, i.Name})
		j, _ := json.Marshal(i)
		expected = append(expected, string(j))
	}

	makeTempFile(dir, name, records...)
	return
}

func makeTempFile(dir, name string, records ...[]string) {
	path :=filepath.Join(dir, name)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	Expect(err).ToNot(HaveOccurred())
	defer file.Close()

	w := csv.NewWriter(file)
	for _, record := range records {
		Expect(w.Write(record)).To(Succeed())
	}

	w.Flush()

	Expect(w.Error()).ToNot(HaveOccurred())
}

