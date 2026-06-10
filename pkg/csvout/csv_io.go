package csvout

import (
	"bufio"
	"compress/gzip"
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

func createAtomicTempPath(outputPath string) (string, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return "", err
	}

	pattern := filepath.Base(outputPath) + ".tmp-*"
	if isParquetPath(outputPath) {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".parquet"))
		pattern = base + ".tmp-*.parquet"
	} else if isGzipPath(outputPath) {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".gz"))
		pattern = base + ".tmp-*.gz"
	}

	file, err := os.CreateTemp(filepath.Dir(outputPath), pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func replaceFile(sourcePath string, targetPath string) error {
	if err := ensureParentDir(targetPath); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		if err := os.Remove(targetPath); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(sourcePath, targetPath)
}

func (c *Config) createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, nil, err
	}

	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if isGzipPath(outputPath) {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := c.csvWriterFactory(gzipWriter)
		closeWriter := func() error {
			writer.Flush()
			if err := writer.Error(); err != nil {
				gzipWriter.Close()
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := gzipWriter.Close(); err != nil {
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := bufWriter.Flush(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, writer, closeWriter, nil
	}

	writer := c.csvWriterFactory(bufWriter)
	closeWriter := func() error {
		writer.Flush()
		if err := writer.Error(); err != nil {
			bufWriter.Flush()
			file.Close()
			return err
		}
		if err := bufWriter.Flush(); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	}
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
			if err := gzipReader.Close(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
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

	bufWriter := bufio.NewWriterSize(file, 64*1024)

	if isGzipPath(path) {
		gzipWriter := gzip.NewWriter(bufWriter)
		writer := c.csvWriterFactory(gzipWriter)
		closeWriter := func() error {
			writer.Flush()
			if err := writer.Error(); err != nil {
				gzipWriter.Close()
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := gzipWriter.Close(); err != nil {
				bufWriter.Flush()
				file.Close()
				return err
			}
			if err := bufWriter.Flush(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, writer, closeWriter, nil
	}

	writer := c.csvWriterFactory(bufWriter)
	closeWriter := func() error {
		writer.Flush()
		if err := writer.Error(); err != nil {
			bufWriter.Flush()
			file.Close()
			return err
		}
		if err := bufWriter.Flush(); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	}
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

func openAppendCSVWriter(path string) (*os.File, csvRecordWriter, func() error, error) {
	return DefaultConfig().openAppendCSVWriter(path)
}

func createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	return DefaultConfig().createCSVWriter(outputPath)
}
