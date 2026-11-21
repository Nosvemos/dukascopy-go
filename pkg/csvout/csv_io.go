package csvout

import (
	"bufio"
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func ensureParentDir(outputPath string) error {
	dir := filepath.Dir(outputPath)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

// knownOutputExtensions are the file extensions whose position at the end of a
// path drives format detection. They must survive in atomic temp file names.
var knownOutputExtensions = map[string]struct{}{
	".csv":     {},
	".tsv":     {},
	".txt":     {},
	".gz":      {},
	".parquet": {},
	".arrow":   {},
	".ipc":     {},
	".feather": {},
	".json":    {},
	".jsonl":   {},
}

// outputExtensionChain returns the trailing run of known extensions of name,
// e.g. ".csv.gz" for "x.csv.gz" and ".arrow" for "2024.01.arrow".
func outputExtensionChain(name string) string {
	chain := ""
	rest := name
	for {
		ext := filepath.Ext(rest)
		if ext == "" || ext == rest {
			return chain
		}
		if _, ok := knownOutputExtensions[strings.ToLower(ext)]; !ok {
			return chain
		}
		chain = ext + chain
		rest = strings.TrimSuffix(rest, ext)
	}
}

// atomicTempMode matches the permissions os.Create gives to non-atomic outputs
// (before umask), so atomic and non-atomic writes produce the same mode.
const atomicTempMode = 0o644

func createAtomicTempPath(outputPath string) (string, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return "", err
	}

	name := filepath.Base(outputPath)
	chain := outputExtensionChain(name)
	pattern := strings.TrimSuffix(name, chain) + ".tmp-*" + chain

	file, err := os.CreateTemp(filepath.Dir(outputPath), pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Chmod(atomicTempMode); err != nil {
		file.Close()
		os.Remove(path)
		return "", err
	}
	if err := file.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

// replaceFile moves sourcePath over targetPath. os.Rename replaces an existing
// target atomically on POSIX and on Windows (MoveFileEx with
// MOVEFILE_REPLACE_EXISTING), so the target is never removed before the new
// data is in place.
func replaceFile(sourcePath string, targetPath string) error {
	if err := ensureParentDir(targetPath); err != nil {
		return err
	}
	return os.Rename(sourcePath, targetPath)
}

// syncPath flushes a closed file's contents to stable storage.
func syncPath(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

// commitAtomic syncs a fully written temp file and moves it over targetPath.
func commitAtomic(tempPath string, targetPath string) error {
	if err := syncPath(tempPath); err != nil {
		return err
	}
	return replaceFile(tempPath, targetPath)
}

// wrapCSVFileWriter layers buffering (and gzip when gzipped) over file and
// returns a CSV writer plus a close function that flushes every layer and
// reports the first failure joined with any later ones.
func (c *Config) wrapCSVFileWriter(file *os.File, gzipped bool) (csvRecordWriter, func() error) {
	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if gzipped {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := c.csvWriterFactory(gzipWriter)
		closeWriter := func() error {
			writer.Flush()
			writerErr := writer.Error()
			gzipErr := gzipWriter.Close()
			flushErr := bufWriter.Flush()
			closeErr := file.Close()
			return errors.Join(writerErr, gzipErr, flushErr, closeErr)
		}
		return writer, closeWriter
	}

	writer := c.csvWriterFactory(bufWriter)
	closeWriter := func() error {
		writer.Flush()
		writerErr := writer.Error()
		flushErr := bufWriter.Flush()
		closeErr := file.Close()
		return errors.Join(writerErr, flushErr, closeErr)
	}
	return writer, closeWriter
}

func (c *Config) createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, nil, err
	}
	writer, closeWriter := c.wrapCSVFileWriter(file, isGzipPath(outputPath))
	return file, writer, closeWriter, nil
}

func (c *Config) openCSVReader(path string) (*os.File, csvReaderFactoryFunc, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}

	bufReader := bufio.NewReaderSize(file, 64*1024)

	if isGzipPath(path) {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			file.Close()
			return nil, nil, nil, err
		}
		closeReader := func() error {
			return errors.Join(gzipReader.Close(), file.Close())
		}
		return file, func() csvRecordReader { return c.csvReaderFactory(gzipReader) }, closeReader, nil
	}

	closeReader := func() error {
		return file.Close()
	}
	return file, func() csvRecordReader { return c.csvReaderFactory(bufReader) }, closeReader, nil
}

type csvReaderFactoryFunc func() csvRecordReader

func (c *Config) openAppendCSVWriter(path string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	writer, closeWriter := c.wrapCSVFileWriter(file, isGzipPath(path))
	return file, writer, closeWriter, nil
}

func isGzipPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(path)), ".gz")
}

func openCSVReader(path string) (*os.File, csvRecordReader, func() error, error) {
	file, readerFactory, closeReader, err := DefaultConfig().openCSVReader(path)
	if err != nil {
		return nil, nil, nil, err
	}
	return file, readerFactory(), closeReader, nil
}

func createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	return DefaultConfig().createCSVWriter(outputPath)
}
