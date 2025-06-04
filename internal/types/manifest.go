package types

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
	Digest string
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
