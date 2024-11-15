package main

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type Config struct {
	AccountName    string
	ContainerName  string
	BlobName       string
	OutputPath     string
	ChunkSize      int64
	Quiet          bool
	SkipValidation bool
}

type DownloadProgress struct {
	TotalBytes      int64
	DownloadedBytes int64
}

func (dp *DownloadProgress) GetPercentComplete() float64 {
	if dp.TotalBytes == 0 {
		return 0
	}
	return float64(dp.DownloadedBytes) / float64(dp.TotalBytes) * 100
}

type BlobInfo struct {
	Size         int64
	ContentMD5   []byte
	ContentType  string
	LastModified time.Time
}

func main() {
	config, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Create context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		fmt.Println("\nReceived interrupt signal. Canceling download...")
		cancel()
	}()

	if err := downloadWithProgressAndValidation(ctx, config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() (*Config, error) {
	var config Config
	var chunkSizeMB int

	flag.StringVar(&config.AccountName, "account", "", "Azure Storage account name")
	flag.StringVar(&config.ContainerName, "container", "", "Container name")
	flag.StringVar(&config.BlobName, "blob", "", "Blob name/path")
	flag.StringVar(&config.OutputPath, "output", "", "Output file path (default: same as blob name)")
	flag.IntVar(&chunkSizeMB, "chunk-size", 4, "Chunk size in MB (default: 4)")
	flag.BoolVar(&config.Quiet, "quiet", false, "Suppress progress output")
	flag.BoolVar(&config.SkipValidation, "skip-validation", false, "Skip MD5 validation")

	// Add help message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEnvironment Variables:\n")
		fmt.Fprintf(os.Stderr, "  AZURE_CLIENT_ID      Service principal client ID\n")
		fmt.Fprintf(os.Stderr, "  AZURE_TENANT_ID      Azure tenant ID\n")
		fmt.Fprintf(os.Stderr, "  AZURE_CLIENT_SECRET  Service principal secret\n")
	}

	flag.Parse()

	// Validate required flags
	if config.AccountName == "" {
		return nil, fmt.Errorf("account name is required")
	}
	if config.ContainerName == "" {
		return nil, fmt.Errorf("container name is required")
	}
	if config.BlobName == "" {
		return nil, fmt.Errorf("blob name is required")
	}

	// Set default output path if not specified
	if config.OutputPath == "" {
		config.OutputPath = filepath.Base(config.BlobName)
	}

	// Convert chunk size to bytes
	config.ChunkSize = int64(chunkSizeMB) * 1024 * 1024

	return &config, nil
}

func downloadAndValidateBlobToFile(ctx context.Context, client *azblob.Client, config *Config, progress *DownloadProgress) error {
	// Get blob properties
	containerClient := client.ServiceClient().NewContainerClient(config.ContainerName)

	blobClient := containerClient.NewBlobClient(config.BlobName)

	props, err := blobClient.GetProperties(ctx, nil)

	if err != nil {
		return fmt.Errorf("failed to get blob properties: %v", err)
	}

	blobInfo := BlobInfo{
		Size:         *props.ContentLength,
		ContentMD5:   props.ContentMD5,
		ContentType:  *props.ContentType,
		LastModified: *props.LastModified,
	}

	progress.TotalBytes = blobInfo.Size

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(config.OutputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Create the destination file
	file, err := os.Create(config.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	var multiWriter io.Writer = file
	var md5Hash hash.Hash

	if !config.SkipValidation {
		md5Hash = md5.New()
		multiWriter = io.MultiWriter(file, md5Hash)
	}

	// Download the blob in chunks
	for offset := int64(0); offset < blobInfo.Size; offset += config.ChunkSize {
		select {
		case <-ctx.Done():
			return fmt.Errorf("download canceled")
		default:
			length := config.ChunkSize
			if offset+length > blobInfo.Size {
				length = blobInfo.Size - offset
			}

			downloadOptions := &azblob.DownloadStreamOptions{
				Range: azblob.HTTPRange{
					Offset: offset,
					Count:  length,
				},
			}

			blobDownloadResponse, err := client.DownloadStream(ctx, config.ContainerName, config.BlobName, downloadOptions)
			if err != nil {
				return fmt.Errorf("failed to download chunk at offset %d: %v", offset, err)
			}

			written, err := io.Copy(multiWriter, &progressReader{
				reader:   blobDownloadResponse.Body,
				progress: progress,
			})
			blobDownloadResponse.Body.Close()

			if err != nil {
				return fmt.Errorf("failed to write chunk at offset %d: %v", offset, err)
			}

			if written != length {
				return fmt.Errorf("incomplete chunk write at offset %d: got %d bytes, expected %d", offset, written, length)
			}
		}
	}

	// Validate the download if not skipped
	if !config.SkipValidation {
		if err := validateDownload(blobInfo, md5Hash.Sum(nil), config.OutputPath); err != nil {
			os.Remove(config.OutputPath)
			return fmt.Errorf("validation failed: %v", err)
		}
	}

	return nil
}

func validateDownload(blobInfo BlobInfo, downloadedMD5 []byte, filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}

	if fileInfo.Size() != blobInfo.Size {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", blobInfo.Size, fileInfo.Size())
	}

	if len(blobInfo.ContentMD5) > 0 && len(downloadedMD5) > 0 {
		if string(downloadedMD5) != string(blobInfo.ContentMD5) {
			return fmt.Errorf("MD5 hash mismatch: expected %s, got %s",
				base64.StdEncoding.EncodeToString(blobInfo.ContentMD5),
				base64.StdEncoding.EncodeToString(downloadedMD5))
		}
	}

	return nil
}

type progressReader struct {
	reader   io.Reader
	progress *DownloadProgress
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.progress.DownloadedBytes = pr.progress.DownloadedBytes + int64(n)
	}
	return n, err
}

func downloadWithProgressAndValidation(ctx context.Context, config *Config) error {
	// Create a default Azure credential
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create credential: %v", err)
	}

	// Create the service URL and client
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", config.AccountName)
	client, err := azblob.NewClient(serviceURL, credential, nil)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	progress := &DownloadProgress{}
	done := make(chan error)

	go func() {
		done <- downloadAndValidateBlobToFile(ctx, client, config, progress)
	}()

	if !config.Quiet {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case err := <-done:
				if err != nil {
					return err
				}
				fmt.Printf("\rDownload completed and validated! Total bytes: %d\n", progress.TotalBytes)
				return nil
			case <-ticker.C:
				fmt.Printf("\rProgress: %.2f%% (%d/%d bytes)",
					progress.GetPercentComplete(),
					atomic.LoadInt64(&progress.DownloadedBytes),
					progress.TotalBytes)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return <-done
}

// To execute this utility run the following cmd after building the code

/*
sudo go run download.go -account az104storagesh -container executable -blob \
create-vm -output /opt/new-download/create-vm -chunk-size 8 -quiet -skip-validation
*/
