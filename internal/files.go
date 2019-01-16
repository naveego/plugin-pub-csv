 package internal
//
// import (
// 	"archive/zip"
// 	"fmt"
// 	"io"
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"strings"
//
// 	uuid "github.com/satori/go.uuid"
// )
//
// func unmanglePath(path string) string {
// 	prefixLength := len(os.TempDir()) + 37 + 2 + 7
// 	if len(path) < prefixLength {
// 		return path
// 	}
// 	return path[prefixLength:]
// }
//
// func getAllMatchingFiles(path string) ([]string, error) {
// 	if !filepath.IsAbs(path) {
// 		return nil, fmt.Errorf("%s is not an absolute path", path)
// 	}
//
// 	files, err := filepath.Glob(path)
// 	return files, err
// }
//
// func prepareFilesForProcessing(path string) (tmpdir string, files []string, err error) {
//
// 	inputFiles, err := getAllMatchingFiles(path)
// 	if err != nil {
// 		return "", nil, err
// 	}
//
// 	u := uuid.NewV4()
// 	tmpdir = filepath.Join(os.TempDir(), "pub-csv", u.String())
// 	if err = os.MkdirAll(tmpdir, 0777); err != nil {
// 		return tmpdir, nil, fmt.Errorf("couldn't create temp directory %s: %s", tmpdir, err)
// 	}
//
// 	log.Printf("preparing files for processing: %v", inputFiles)
//
// 	for _, inputFile := range inputFiles {
// 		if strings.HasSuffix(inputFile, ".zip") {
// 			unzippedFiles, err := unzipMatchedFiles(inputFile, tmpdir)
// 			if err != nil {
// 				return "", nil, fmt.Errorf("couldn't open zip file '%s': %s", inputFile, err)
// 			}
// 			files = append(files, unzippedFiles...)
// 			continue
// 		}
//
// 		file := createTempFilePath(tmpdir, inputFile)
//
// 		if err := copyFileContents(inputFile, file); err != nil {
// 			return "", nil, fmt.Errorf("couldn't copy file %s to temp directory for processing: %s", inputFile, err)
// 		}
//
// 		files = append(files, file)
// 	}
//
// 	return tmpdir, files, nil
// }
//
// func unzipMatchedFiles(zipPath string, tmpdir string) ([]string, error) {
// 	log.Printf("unzipping files in %s to %s", zipPath, tmpdir)
// 	r, err := zip.OpenReader(zipPath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer r.Close()
//
// 	var unzipped []string
//
// 	// Iterate through the files in the archive,
// 	// printing some of their contents.
// 	for _, f := range r.File {
//
// 		fileName := filepath.Join(zipPath, f.Name)
// 		unzippedPath := createTempFilePath(tmpdir, fileName)
// 		fileReader, err := f.Open()
// 		if err != nil {
// 			return nil, fmt.Errorf("couldn't extract file '%s' from zip '%s' to temporary directory: %s", f.Name, zipPath, err)
//
// 		}
// 		err = copyToFile(fileReader, unzippedPath)
// 		fileReader.Close()
// 		if err != nil {
// 			return nil, fmt.Errorf("couldn't copy extracted file '%s' from zip '%s' to temporary directory: %s", f.Name, zipPath, err)
// 		}
// 		unzipped = append(unzipped, unzippedPath)
// 	}
//
// 	return unzipped, nil
// }
//
// func createTempFilePath(tmpdir, path string) string {
// 	v := filepath.VolumeName(path)
// 	rel := path[len(v):]
// 	return filepath.Join(tmpdir, rel)
// }
//
// func copyToFile(in io.Reader, dst string) (err error) {
//
// 	if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
// 		return
// 	}
//
// 	out, err := os.Create(dst)
// 	if err != nil {
// 		return
// 	}
// 	defer func() {
// 		cerr := out.Close()
// 		if err == nil {
// 			err = cerr
// 		}
// 	}()
// 	if _, err = io.Copy(out, in); err != nil {
// 		return
// 	}
// 	err = out.Sync()
// 	return
// }
//
// // copyFileContents copies the contents of the file named src to the file named
// // by dst. The file will be created if it does not already exist. If the
// // destination file exists, all it's contents will be replaced by the contents
// // of the source file.
// func copyFileContents(src, dst string) (err error) {
// 	in, err := os.Open(src)
// 	if err != nil {
// 		return
// 	}
// 	defer in.Close()
// 	if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
// 		return
// 	}
// 	out, err := os.Create(dst)
// 	if err != nil {
// 		return
// 	}
// 	defer func() {
// 		cerr := out.Close()
// 		if err == nil {
// 			err = cerr
// 		}
// 	}()
// 	if _, err = io.Copy(out, in); err != nil {
// 		return
// 	}
// 	err = out.Sync()
// 	return
// }
