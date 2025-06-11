package types

import (
	"bytes"
	"encoding/json"
	"os"
	"time"
)

//go:generate easyjson -all manifest.go
type Manifest struct {
	ContentType string
	Data        []byte
}

//go:generate easyjson -all manifest.go
type MultiManifest struct {
	TotalChunks int
	Digest      string
	Size        int
}

type ManifestV2Config struct {
	MediaType string
	Size      int
	Digest    string
}

//go:generate easyjson -all manifest.go
type ManifestV2Content struct {
	ID    string
	Dir   string
	Files map[string]ManifestV2File
}

//go:generate easyjson -all manifest.go
type ManifestV2File struct {
	ID         uint64
	Name       string
	Size       int64
	ModifyTime time.Duration
}

//go:generate easyjson -all manifest.go
type ManifestV2 struct {
	SchemaVersion int
	Config        ManifestV2Config
	Layers        []ManifestV2Config
}
type ManifestV1Config struct {
	BlobSum string
}

//go:generate easyjson -all manifest.go
type ManifestV1 struct {
	SchemaVersion int
	FSLayers      []ManifestV1Config
}

func (m *ManifestV2) SavePrettyJSON(filename string) error {
	data, err := m.MarshalJSON()
	if err != nil {
		return err
	}

	var prettyJSON bytes.Buffer
	err = json.Indent(&prettyJSON, data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, prettyJSON.Bytes(), 0o666)
}
