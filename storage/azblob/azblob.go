package azblob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/sourcegraph/checkup/storage/fs"
	"github.com/sourcegraph/checkup/types"
)

// Type should match the package name
const Type = "azblob"

// Storage is a way to store checkup results in an S3 bucket.
type Storage struct {
	// SASURL is a Shared Access Signature URL
	SASURL string `json:"sas_url"`

	// AccountName specifies the name of the Azure Storage account.
	// Used by provision command.
	AccountName string `json:"account_name"`

	// AccountKey specifies a valid access key for the AccountName
	// Azure Storage account.  Used by provision command.
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

	// TODO: add SAS URL support
	// credentials := azblob.NewAnonymousCredential()
	credentials, err := azblob.NewSharedKeyCredential(s.AccountName, s.AccountKey)
	if err != nil {
		errmsg := fmt.Errorf("Cannot create Azure Storage credential: %w", err)
		log.Fatal(errmsg)
	}

	p := azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", s.ContainerName))
	serviceURL := azblob.NewServiceURL(*u, p)

	ctx := context.Background()
	containerSvc := newAzContainer(serviceURL, s.ContainerName)
	blobSvc := containerSvc.NewBlockBlobURL(*fs.GenerateFilename())
	resp, err := blobSvc.Upload(ctx, bytes.NewReader(jsonBytes), azblob.BlobHTTPHeaders{ContentType: "application/json"}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		errmsg := fmt.Errorf("Cannot upload Azure Blob: %w (request-id %s) %s", err, resp.RequestID(), resp.ErrorCode())
		log.Fatal(errmsg)
	}
	return err
}

// Maintain deletes check files that are older than s.CheckExpiry.
func (s Storage) Maintain() error {
	if s.CheckExpiry == 0 {
		return nil
	}

	// TODO: add SAS URL support
	// credentials := azblob.NewAnonymousCredential()
	if s.AccountName == "" || s.AccountKey == "" {
		log.Fatal("Must supply both a valid Azure Storage Account Name and Account Key")
	}
	credentials, err := azblob.NewSharedKeyCredential(s.AccountName, s.AccountKey)
	if err != nil {
		errmsg := fmt.Errorf("Cannot create Azure Storage credential: %w", err)
		log.Fatal(errmsg)
	}

	p := azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", s.AccountName))
	serviceURL := azblob.NewServiceURL(*u, p)

	ctx := context.Background()
	containerSvc := serviceURL.NewContainerURL(s.ContainerName)
	blobsToDelete := []azblob.BlobItem{}

	// List blobs and mark those older than s.CheckExpiry
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerSvc.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			errmsg := fmt.Errorf("Cannot list Azure Blob container %s: %w", listBlob.ContainerName, err)
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
		blobSvc := containerSvc.NewBlockBlobURL(del.Name)
		_, err := blobSvc.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			errmsg := fmt.Errorf("Cannot delete blobs in Azure Blob container: %w", err)
			log.Fatal(errmsg)
		}
	}
	return nil
}

// Provision creates a new Azure Storage Account container
// for the storage account specified by s, and then creates a new
// Shared Access Signature with proper permissions for Checkup.
// The credentials in s must have the IAMFullAccess and
// AmazonS3FullAccess permissions in order to succeed.
//
// Provision need only be called once per status page (bucket),
// not once per endpoint.
func (s Storage) Provision() (types.ProvisionInfo, error) {
	var info types.ProvisionInfo

	// TODO: add SAS URL support
	if s.AccountName == "" || s.AccountKey == "" {
		log.Fatal("Must supply both a valid Azure Storage Account Name and Account Key")
	}
	credentials, err := azblob.NewSharedKeyCredential(s.AccountName, s.AccountKey)
	if err != nil {
		errmsg := fmt.Errorf("Cannot create Azure Storage credential: %w", err)
		log.Fatal(errmsg)
	}

	p := azblob.NewPipeline(credentials, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", s.AccountName))
	serviceURL := azblob.NewServiceURL(*u, p)

	ctx := context.Background()
	//validStorageAccount     = regexp.MustCompile("^[0-9a-z]{3,24}$")
	containerSvc := newAzContainer(serviceURL, s.ContainerName)

	resp, err := containerSvc.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if resp.StatusCode() != 409 && err != nil {
		errmsg := fmt.Errorf("Cannot create Azure Blob container %s: %w", s.ContainerName, err)
		log.Fatal(errmsg)
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
	setPropResp, err := serviceURL.SetProperties(ctx, properties)
	if setPropResp.StatusCode() != 409 && err != nil {
		errmsg := fmt.Errorf("Cannot set CORS rule on Azure Storage container: %w", err)
		log.Fatal(errmsg)
	}
	// Get SAS and print on CLI or add to config
	permissions := azblob.BlobSASPermissions{Read: true}.String()
	sasQueryParams := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
		ContainerName: s.ContainerName,
		Permissions:   permissions,
	}
	queryParams, err := sasQueryParams.NewSASQueryParameters(credentials)
	if err != nil {
		errmsg := fmt.Errorf("Cannot genrate SAS URL for Azure Storage container: %w", err)
		log.Fatal(errmsg)
	}

	qp := queryParams.Encode()
	u, err = url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s/?%s",
		s.AccountName, s.ContainerName, qp))
	if err != nil {
		log.Fatal(err)
	}

	info.AzureStorageSASURL = *u
	return info, nil
}

// newAzContainer calls azblob.NewContainerURL(), but may be replaced for mocking in tests.
var newAzContainer = func(serviceURL azblob.ServiceURL, container string) azContainerSvc {
	return serviceURL.NewContainerURL(container)
}

// azBlobSvc is used for mocking the azblob.BlockBlobURL type.
type azBlobSvc interface {
	Upload(context.Context, io.ReadSeeker, azblob.BlobHTTPHeaders, azblob.Metadata, azblob.BlobAccessConditions) (*azblob.BlockBlobUploadResponse, error)
	Delete(context.Context, azblob.DeleteSnapshotsOptionType, azblob.BlobAccessConditions) (*azblob.BlobDeleteResponse, error)
}

// azContainerSvc is used for mocking the azblob.ContainerURL type.
type azContainerSvc interface {
	Create(context.Context, azblob.Metadata, azblob.PublicAccessType) (*azblob.ContainerCreateResponse, error)
	ListBlobsFlatSegment(context.Context, azblob.Marker, azblob.ListBlobsSegmentOptions) (*azblob.ListBlobsFlatSegmentResponse, error)
	NewBlockBlobURL(string) azblob.BlockBlobURL
}
