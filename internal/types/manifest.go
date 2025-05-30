package types

//go:generate easyjson -all person.go
type Manifest struct {
	ContentType string
	Data        []byte
}

//go:generate easyjson -all person.go
type MultiManifest struct {
	TotalChunks int
	Digest      string
	Size        int
}

//go:generate easyjson -all person.go
type ManifestV2 struct {
	SchemaVersion int
	Config        struct {
		Digest string
	}
	Layers []struct {
		Digest string
	}
}

//go:generate easyjson -all person.go
type ManifestV1 struct {
	SchemaVersion int
	FSLayers      []struct {
		BlobSum string
	}
}
