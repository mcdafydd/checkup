package azblob

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/sourcegraph/checkup/types"
)

func TestAzblobStoreGetSAS(t *testing.T) {
	accountName, accountKey, containerName := "fakeName", []byte("fakeKey"), "fakeContainer"
	fakeazblob := new(azblobMock)
	results := []types.Result{{Title: "Testing"}}
	resultsBytes := []byte(`[{"title":"Testing"}]`)
	newBlockBlobURL = func(url url.URL, p pipeline.Pipeline) azBlobSvc {
		fakeazblob.BlobURL = url
		return fakeazblob
	}

	specimen := Storage{
		AccountName:   accountName,
		AccountKey:    base64.StdEncoding.EncodeToString(accountKey),
		ContainerName: containerName,
	}
	err := specimen.Store(results)
	if err != nil {
		t.Fatalf("Expected no error from Store(), got: %v", err)
	}

	// Make sure container name is right
	newBlobURLParts := azblob.NewBlobURLParts(fakeazblob.BlobURL)
	if got, want := newBlobURLParts.ContainerName, containerName; got != want {
		t.Errorf("Expected Container to be '%s', got '%s'", want, got)
	}

	// Make sure filename has timestamp of check
	key := newBlobURLParts.BlobName
	hyphenPos := strings.Index(key, "-")
	if hyphenPos < 0 {
		t.Fatalf("Expected Key to have timestamp then hyphen, got: %s", key)
	}
	tsString := key[:hyphenPos]
	tsNs, err := strconv.ParseInt(tsString, 10, 64)
	if err != nil {
		t.Fatalf("Expected Key's timestamp to be integer; got error: %v", err)
	}
	ts := time.Unix(0, tsNs)
	if time.Since(ts) > 1*time.Second {
		t.Errorf("Timestamp of filename is %s but expected something very recent", ts)
	}

	// Make sure body bytes are correct
	bodyBytes, err := ioutil.ReadAll(fakeazblob.input.Body)
	if err != nil {
		t.Fatalf("Expected no error reading body, got: %v", err)
	}
	if bytes.Compare(bodyBytes, resultsBytes) != 0 {
		t.Errorf("Contents of file are wrong\nExpected %s\n     Got %s", resultsBytes, bodyBytes)
	}
}

func TestAzblobStoreCheckSASValid(t *testing.T) {
	accountName, accountKey, containerName := "fakeName", []byte("fakeKey"), "fakeContainer"
	sasURLstr := fmt.Sprintf("https://%s.blob.core.windows.net/%s?sv=2030-10-10&ss=b&srt=co&sp=rwdlacx&se=2030-05-10T05:36:04Z&st=2030-05-09T21:36:04Z&spr=https&sig=ORTn9UO1zx%%2F0xQ%%2BTTPa0E%%2FzO1TD95E3btAJQTadZaeU%%3D", accountName, containerName)
	sasURL, _ := url.Parse(sasURLstr)
	fakeazblob := new(azblobMock)
	fakeazblob.BlobURL = *sasURL
	results := []types.Result{{Title: "Testing"}}
	resultsBytes := []byte(`[{"title":"Testing"}]`)
	newBlockBlobURL = func(url url.URL, p pipeline.Pipeline) azBlobSvc {
		fakeazblob.BlobURL = url
		return fakeazblob
	}

	specimen := Storage{
		SASURL:        sasURL,
		AccountName:   accountName,
		AccountKey:    base64.StdEncoding.EncodeToString(accountKey),
		ContainerName: containerName,
	}

	err := specimen.Store(results)
	if err != nil {
		t.Fatalf("Expected no error from Store(), got: %v", err)
	}

	// Make sure SASURL still exists
	if specimen.SASURL == nil {
		t.Fatalf("Expected SASURL to exist, got: %v", specimen.SASURL)
	}

	// Make sure container name is right
	newBlobURLParts := azblob.NewBlobURLParts(fakeazblob.BlobURL)
	if got, want := newBlobURLParts.ContainerName, containerName; got != want {
		t.Errorf("Expected Container to be '%s', got '%s'", want, got)
	}

	// Make sure filename has timestamp of check
	key := newBlobURLParts.BlobName
	hyphenPos := strings.Index(key, "-")
	if hyphenPos < 0 {
		t.Fatalf("Expected Key to have timestamp then hyphen, got: %s", key)
	}
	tsString := key[:hyphenPos]
	tsNs, err := strconv.ParseInt(tsString, 10, 64)
	if err != nil {
		t.Fatalf("Expected Key's timestamp to be integer; got error: %v", err)
	}
	ts := time.Unix(0, tsNs)
	if time.Since(ts) > 1*time.Second {
		t.Errorf("Timestamp of filename is %s but expected something very recent", ts)
	}

	// Make sure body bytes are correct
	bodyBytes, err := ioutil.ReadAll(fakeazblob.input.Body)
	if err != nil {
		t.Fatalf("Expected no error reading body, got: %v", err)
	}
	if bytes.Compare(bodyBytes, resultsBytes) != 0 {
		t.Errorf("Contents of file are wrong\nExpected %s\n     Got %s", resultsBytes, bodyBytes)
	}
}

func TestAzblobStoreCheckSASExpired(t *testing.T) {
	accountName, accountKey, containerName := "fakeName", []byte("fakeKey"), "fakeContainer"
	sasURLstr := fmt.Sprintf("https://%s.blob.core.windows.net/%s?sv=2030-10-10&ss=b&srt=co&sp=rwdlacx&se=1999-05-10T05:36:04Z&st=2030-05-09T21:36:04Z&spr=https&sig=ORTn9UO1zx%%2F0xQ%%2BTTPa0E%%2FzO1TD95E3btAJQTadZaeU%%3D", accountName, containerName)
	sasURL, _ := url.Parse(sasURLstr)
	fakeazblob := new(azblobMock)
	fakeazblob.BlobURL = *sasURL
	results := []types.Result{{Title: "Testing"}}
	resultsBytes := []byte(`[{"title":"Testing"}]`)
	newBlockBlobURL = func(url url.URL, p pipeline.Pipeline) azBlobSvc {
		fakeazblob.BlobURL = url
		return fakeazblob
	}

	specimen := Storage{
		SASURL:        sasURL,
		AccountName:   accountName,
		AccountKey:    base64.StdEncoding.EncodeToString(accountKey),
		ContainerName: containerName,
	}
	err := specimen.Store(results)
	if err != nil {
		t.Fatalf("Expected no error from Store(), got: %v", err)
	}

	// Make sure SASURL still exists and has valid timestamp
	if specimen.SASURL == nil {
		t.Fatalf("Expected SASURL to exist, got: %v", specimen.SASURL)
	}

	// Make sure container name is right
	newBlobURLParts := azblob.NewBlobURLParts(fakeazblob.BlobURL)
	if got, want := newBlobURLParts.ContainerName, containerName; got != want {
		t.Errorf("Expected Container to be '%s', got '%s'", want, got)
	}

	// Make sure filename has timestamp of check
	key := newBlobURLParts.BlobName
	hyphenPos := strings.Index(key, "-")
	if hyphenPos < 0 {
		t.Fatalf("Expected Key to have timestamp then hyphen, got: %s", key)
	}
	tsString := key[:hyphenPos]
	tsNs, err := strconv.ParseInt(tsString, 10, 64)
	if err != nil {
		t.Fatalf("Expected Key's timestamp to be integer; got error: %v", err)
	}
	ts := time.Unix(0, tsNs)
	if time.Since(ts) > 1*time.Second {
		t.Errorf("Timestamp of filename is %s but expected something very recent", ts)
	}

	// Make sure body bytes are correct
	bodyBytes, err := ioutil.ReadAll(fakeazblob.input.Body)
	if err != nil {
		t.Fatalf("Expected no error reading body, got: %v", err)
	}
	if bytes.Compare(bodyBytes, resultsBytes) != 0 {
		t.Errorf("Contents of file are wrong\nExpected %s\n     Got %s", resultsBytes, bodyBytes)
	}
}

func TestAzblobMaintain(t *testing.T) {
	accountName, accountKey, containerName := "fakeName", []byte("fakeKey"), "fakeContainer"
	sasURLstr := fmt.Sprintf("https://%s.blob.core.windows.net/%s?sv=2030-10-10&ss=b&srt=co&sp=rwdlacx&se=2030-05-10T05:36:04Z&st=2030-05-09T21:36:04Z&spr=https&sig=ORTn9UO1zx%%2F0xQ%%2BTTPa0E%%2FzO1TD95E3btAJQTadZaeU%%3D", accountName, containerName)
	sasURL, _ := url.Parse(sasURLstr)
	fakeazcontainer := new(azcontainerMock)
	fakeazblob := new(azblobMock)
	newAzContainer = func(serviceURL azblob.ServiceURL, container string) azContainerSvc {
		return fakeazcontainer
	}
	newBlockBlobURL = func(url url.URL, p pipeline.Pipeline) azBlobSvc {
		return fakeazblob
	}

	specimen := Storage{
		SASURL:        sasURL,
		AccountName:   accountName,
		AccountKey:    base64.StdEncoding.EncodeToString(accountKey),
		ContainerName: containerName,
	}
	err := specimen.Maintain()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if fakeazblob.deleted {
		t.Fatal("No deletions should happen unless CheckExpiry is set")
	}

	specimen.CheckExpiry = 24 * 30 * time.Hour
	err = specimen.Maintain()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !fakeazblob.deleted {
		t.Fatal("Expected deletions, but there weren't any")
	}
}

// azcontainerMock mocks azblob.ContainerURL.
type azcontainerMock struct {
	input azContainerSvc
}

// azblobMock mocks azblob.BlockBlobURL.
type azblobMock struct {
	BlobURL url.URL
	deleted bool
	input   struct {
		Body     io.ReadSeeker
		h        azblob.BlobHTTPHeaders
		metadata azblob.Metadata
		ac       azblob.BlobAccessConditions
	}
}

func (s Storage) getSASURLMock() (*url.URL, error) {
	url, _ := url.Parse("https://fakeName.blob.core.windows.net/fakeContainer?sv=2030-10-10&ss=b&srt=co&sp=rwdlacx&se=2030-05-10T05:36:04Z&st=2030-05-09T21:36:04Z&spr=https&sig=ORTn9UO1zx%2F0xQ%2BTTPa0E%2FzO1TD95E3btAJQTadZaeU%3D")
	return url, nil
}

func (s *azcontainerMock) Create(ctx context.Context, metadata azblob.Metadata, pa azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error) {
	return nil, nil
}

func (s *azcontainerMock) ListBlobsFlatSegment(context.Context, azblob.Marker, azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsFlatSegmentResponse, error) {
	nextMarker := azblob.Marker{
		Val: new(string),
	}
	*nextMarker.Val = ""
	return &azblob.ListBlobsFlatSegmentResponse{
		NextMarker: nextMarker,
		Segment: azblob.BlobFlatListSegment{
			BlobItems: []azblob.BlobItem{{
				Properties: azblob.BlobProperties{
					LastModified: time.Time{},
				},
			}},
		},
	}, nil
}

func (s *azcontainerMock) NewBlockBlobURL(blobURL string) azblob.BlockBlobURL {
	return s.NewBlockBlobURL(blobURL)
}

func (s *azblobMock) Upload(ctx context.Context, body io.ReadSeeker, h azblob.BlobHTTPHeaders, metadata azblob.Metadata, ac azblob.BlobAccessConditions) (*azblob.BlockBlobUploadResponse, error) {
	s.input.Body = body
	return nil, nil
}

func (s *azblobMock) Delete(ctx context.Context, opt azblob.DeleteSnapshotsOptionType, ac azblob.BlobAccessConditions) (*azblob.BlobDeleteResponse, error) {
	s.deleted = true
	return nil, nil
}
