package types

import (
	"bytes"
	"encoding/json"
	"os"
	"time"
)

//go:generate easyjson -all spec.go
type UID string

//go:generate easyjson -all spec.go
type TypeMeta struct {
	// API 版本定义，格式为"组/版本"，例如 "apps/v1"
	// 对于核心 API，可能只有版本，如 "v1"
	APIVersion string

	// 制品资源类型，如 "RPM", "Binary", "image" 等
	Kind string
}

//go:generate easyjson -all spec.go
type ObjectMeta struct {
	// 对象名称，在命名空间内必须唯一
	Name string

	// 对象所属的命名空间
	Namespace string

	// 系统生成的唯一 ID
	UID UID

	// 资源版本，用于乐观并发控制
	ResourceVersion string

	// 创建时间
	CreationTimestamp time.Time

	// 删除时间，如果设置，表示对象正在被删除
	DeletionTimestamp *time.Time

	// 删除前需要清理的终结器列表
	Finalizers []string

	// 对象的标签，用于选择和组织对象
	Labels map[string]string

	// 对象的注解，存储非标识性辅助数据
	Annotations map[string]string

	// 拥有该对象的引用
	OwnerReferences []OwnerReference

	// 生成名称前缀，用于自动生成名称
	GenerateName string

	// 删除宽限期秒数
	DeletionGracePeriodSeconds *int64

	// 世代计数器，每次规格变更时递增
	Generation int64
}

//go:generate easyjson -all spec.go
type OwnerReference struct {
	// API 版本，与 TypeMeta.APIVersion 相同
	APIVersion string

	// 资源类型，与 TypeMeta.Kind 相同
	Kind string

	// 所有者对象的名称
	Name string

	// 所有者对象的 UID
	UID UID

	// 如果为 true，则此对象被所有者阻止删除
	// 当所有者被删除时，此对象也会被自动删除
	// 默认为 false
	BlockOwnerDeletion *bool

	// 如果为 true，此引用指向的对象是此对象的控制器
	// 一个对象只能有一个控制器
	Controller *bool
}

//go:generate easyjson -all spec.go
type Artifact struct {
	TypeMeta   // 类型元数据：apiVersion, kind
	ObjectMeta // 对象元数据：name, namespace, labels, annotations等

	Spec   ArtifactSpec
	Status ArtifactStatus
}

//go:generate easyjson -all spec.go
type ArtifactSpec struct {
	Dir   string
	Files []FileInfo
}

//go:generate easyjson -all spec.go
type ArtifactStatus struct {
	Tag struct {
		Current string
		History []string
	}
}

type FileInfo struct {
	ID         uint64
	Name       string
	Size       int64
	MD5        string
	ModifyTime time.Duration
	Err        error
}

func (r *Artifact) SavePrettyJSON(filename string) error {
	data, err := r.MarshalJSON()
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
