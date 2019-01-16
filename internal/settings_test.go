package internal_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/naveego/plugin-pub-csv/internal"
)

var _ = Describe("Settings", func() {

	Describe("Validate settings", func() {

		It("Should error if path is not absolute", func() {
			settings := &Settings{RootPath: "bogus"}
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if path is not set", func() {
			settings := &Settings{}
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should set defaults", func() {
			settings := &Settings{RootPath: "/"}
			Expect(settings.Validate()).To(Succeed())
			Expect(settings.Filters).To(BeEquivalentTo([]string{`\.csv$`}))
			Expect(settings.Delimiter).To(BeEquivalentTo(","))
			Expect(settings.CleanupAction).To(Equal(CleanupNothing))
		})
	})
})
