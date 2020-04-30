package azblob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/sourcegraph/checkup/storage/fs"
	"github.com/sourcegraph/checkup/types"
)

// Type should match the package name
const Type = "azblob"

// Storage is a way to store checkup results in an S3 bucket.
type Storage struct {
	// SASURL caches a valid Shared Access Signature URL
	// used by Store().
	SASURL *url.URL `json:"sas_url"`

	// AccountName specifies the name of the Azure Storage account.
	// Used by provision command.
	AccountName string `json:"account_name"`

	// AccountKey specifies a valid access key for the AccountName
	// Azure Storage account.  Used by Provision().
	AccountKey string `json:"account_key"`

	// ContainerName specifies the name of the storage container.
	// This container will be created if it does not exist.
	ContainerName string `json:"container_name"`

	// Check files older than CheckExpiry will be
	// deleted on calls to Maintain(). If this is
	// the zero value, no old check files will be
	// deleted.
	CheckExpiry time.Duration `json:"check_expiry,omitempty"`
}

// New creates a new Storage instance based on json config
func New(config json.RawMessage) (Storage, error) {
	var storage Storage
	err := json.Unmarshal(config, &storage)

	if storage.SASURL == nil {
		u, err := storage.getSASURL()
		if err != nil {
			log.Fatal(err)
		}
		storage.SASURL = u
	}
	if !storage.checkSASURL() {
		log.Fatalf("Failed to get valid storage SAS for storage account %s", storage.AccountName)
	}
	return storage, err
}

// Type returns the storage driver package name
func (Storage) Type() string {
	return Type
}

// Store stores results on S3 according to the configuration in s.
func (s Storage) Store(results []types.Result) error {
	jsonBytes, err := json.Marshal(results)
	if err != nil {
		return err
	}
	if s.SASURL == nil {
		u, err := s.getSASURL()
		if err != nil {
			log.Fatal(err)
		}
		s.SASURL = u
	}
	if !s.checkSASURL() {
		log.Fatalf("Failed to get valid storage SAS for storage account %s", s.AccountName)
	}

	ctx := context.Background()
	newBlobURLParts := azblob.NewBlobURLParts(*s.SASURL)
	newBlobURLParts.BlobName = *fs.GenerateFilename()
	blobURL := azblob.NewBlockBlobURL(newBlobURLParts.URL(), azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
	_, err = blobURL.Upload(ctx, bytes.NewReader(jsonBytes), azblob.BlobHTTPHeaders{ContentType: "application/json"}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		errmsg := fmt.Errorf("Cannot upload Azure Blob: %w", err)
		log.Fatal(errmsg)
	}
	return err
}

// Maintain deletes check files that are older than s.CheckExpiry.
func (s Storage) Maintain() error {
	if s.CheckExpiry == 0 {
		return nil
	}

	if s.AccountName == "" || s.AccountKey == "" {
		log.Fatal("Must supply both a valid Azure Storage Account Name and Account Key")
	}

	credentials, err := azblob.NewSharedKeyCredential(s.AccountName, s.AccountKey)
	if err != nil {
		errmsg := fmt.Errorf("Cannot create Azure Storage credential: %w", err)
		log.Fatal(errmsg)
	}

	ctx := context.Background()
	blobsToDelete := []azblob.BlobItem{}
	p := azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", s.AccountName))
	serviceURL := azblob.NewServiceURL(*u, p)
	containerSvc := newAzContainer(serviceURL, s.ContainerName)

	// List blobs and mark those older than s.CheckExpiry
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerSvc.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			errmsg := fmt.Errorf("Cannot list Azure Blob container: %w", err)
			log.Fatal(errmsg)
		}
		marker = listBlob.NextMarker

		for _, b := range listBlob.Segment.BlobItems {
			if time.Since(b.Properties.LastModified) > s.CheckExpiry {
				blobsToDelete = append(blobsToDelete, b)
			}
		}
	}

	// TODO: Batch API support - https://docs.microsoft.com/en-us/rest/api/storageservices/blob-batch
	for _, del := range blobsToDelete {
		delBlobURLParts := azblob.NewBlobURLParts(*s.SASURL)
		delBlobURLParts.BlobName = del.Name
		blobURL := azblob.NewBlockBlobURL(delBlobURLParts.URL(), azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
		_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			errmsg := fmt.Errorf("Cannot delete blobs in Azure Blob container: %w", err)
			log.Fatal(errmsg)
		}
	}
	return nil
}

// Provision will perform the following steps for the storage account
// specified by s:
//   * Create a new Azure Storage Account container or warn if already exists
//   * Create a Shared Access Signature with read-only permissions at the
//     container-level, valid for one year
//   * Creates a CORS rule for the web application
//
// Provision need only be called once per status page (container),
// not once per endpoint.
func (s Storage) Provision() (types.ProvisionInfo, error) {
	var info types.ProvisionInfo
	validStorageAccount := regexp.MustCompile("^[0-9a-z]{3,24}$")

	if s.AccountName == "" || s.AccountKey == "" {
		log.Fatal("Must supply both a valid Azure Storage Account Name and Account Key")
	}
	if !validStorageAccount.MatchString(s.ContainerName) {
		log.Fatal("Container_name must be between 3 and 24 characters, lowercase, and only contain letters or numbers.")
	}

	credentials, err := azblob.NewSharedKeyCredential(s.AccountName, s.AccountKey)
	if err != nil {
		errmsg := fmt.Errorf("Cannot create Azure Storage credential: %w", err)
		log.Fatal(errmsg)
	}

	p := azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	su, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", s.AccountName))
	serviceURL := azblob.NewServiceURL(*su, p)

	ctx := context.Background()
	containerSvc := newAzContainer(serviceURL, s.ContainerName)
	_, err = containerSvc.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok {
			switch stgErr.ServiceCode() {
			case azblob.ServiceCodeContainerAlreadyExists:
				log.Printf("Warning: Container %s already exists.  Continuing.", s.ContainerName)
			}
		} else {
			log.Fatal(err)
		}
	}

	// Configure its CORS policy to allow reading from status pages
	corsrule := azblob.CorsRule{
		AllowedOrigins:  "*",
		AllowedMethods:  "GET,HEAD",
		AllowedHeaders:  "*",
		ExposedHeaders:  "ETag",
		MaxAgeInSeconds: int32(3000),
	}
	properties := azblob.StorageServiceProperties{
		Cors: []azblob.CorsRule{
			corsrule,
		},
	}
	_, err = serviceURL.SetProperties(ctx, properties)
	if err != nil {
		log.Fatal(err)
	}

	// Create a read-only SAS URL for the newly provisioned container
	sasQueryParams, err := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(365 * 24 * time.Hour),
		ContainerName: s.ContainerName,
		BlobName:      "",
		Permissions:   azblob.BlobSASPermissions{Read: true}.String(),
	}.NewSASQueryParameters(credentials)
	if err != nil {
		return info, err
	}
	qp := sasQueryParams.Encode()
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/?%s",
		s.AccountName, s.ContainerName, qp))
	if err != nil {
		return info, err
	}

	info.AzureStorageSASURL = *u
	return info, err
}

// checkSASURL returns false if SAS expires within 15 minutes or has already expired
func (s Storage) checkSASURL() bool {
	if s.SASURL != nil {
		parts := azblob.NewBlobURLParts(*s.SASURL)
		expiresAt := parts.SAS.ExpiryTime()
		if expiresAt.Before(time.Now().Add(time.Minute * 15)) {
			log.Printf("Warning: SAS expiry within 15 minutes at %s.  Continuing.", expiresAt.Format(time.RFC3339))
			return false
		}
		return true
	}
	return false
}

// getSASURL returns a privileged SAS URL based on the configured Azure Storage account and key
func (s Storage) getSASURL() (*url.URL, error) {
	credentials, err := azblob.NewSharedKeyCredential(s.AccountName, s.AccountKey)
	if err != nil {
		return nil, err
	}

	// BlobName set to "" (default) to indicate we want a container-level credential
	sasQueryParams, err := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
		ContainerName: s.ContainerName,
		BlobName:      "",
		Permissions:   azblob.BlobSASPermissions{Create: true, Delete: true, Add: true, Read: true, Write: true}.String(),
	}.NewSASQueryParameters(credentials)
	if err != nil {
		return nil, err
	}
	qp := sasQueryParams.Encode()
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/?%s",
		s.AccountName, s.ContainerName, qp))
	if err != nil {
		return nil, err
	}

	return u, err
}

// newAzContainer calls azblob.NewContainerURL(), but may be replaced for mocking in tests.
var newAzContainer = func(serviceURL azblob.ServiceURL, container string) azContainerSvc {
	return serviceURL.NewContainerURL(container)
}

// azContainerSvc is used for mocking the azblob.ContainerURL type.
type azContainerSvc interface {
	Create(context.Context, azblob.Metadata, azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error)
	ListBlobsFlatSegment(context.Context, azblob.Marker, azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsFlatSegmentResponse, error)
	NewBlockBlobURL(string) azblob.BlockBlobURL
}
