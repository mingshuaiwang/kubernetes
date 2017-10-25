package quota_volume

import (
	"fmt"

	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&quotaVolumePlugin{nil}}
}

type quotaVolumePlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &quotaVolumePlugin{}
var _ volume.PersistentVolumePlugin = &quotaVolumePlugin{}
var _ volume.DeletableVolumePlugin = &quotaVolumePlugin{}
var _ volume.ProvisionableVolumePlugin = &quotaVolumePlugin{}
var _ volume.ExpandableVolumePlugin = &quotaVolumePlugin{}

const (
	quotaVolumePluginName = "kubernetes.io/quota_volume"
)

// method of VolumePlugin
func (plugin *quotaVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *quotaVolumePlugin) GetPluginName() string {
	return quotaVolumePluginName
}

func (plugin *quotaVolumePlugin) CanSupport(spec *volume.Spec) bool {
	if (spec.Volume != nil && spec.Volume.QuotaVolume == nil) || (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.QuotaVolume == nil) {
		return false
	}

	return true
}

func (plugin *quotaVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v", volumeSource.Name), nil
}

func getVolumeSource(spec *volume.Spec) (*v1.QuotaVolumeSource, error) {
	if spec.Volume != nil && spec.Volume.QuotaVolume != nil {
		return spec.Volume.QuotaVolume, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.QuotaVolume != nil {
		return spec.PersistentVolume.Spec.QuotaVolume, nil
	}

	return nil, fmt.Errorf("Spec does not reference a RBD volume type")
}

func (plugin *quotaVolumePlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	rbdVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			QuotaVolume: &v1.QuotaVolumeSource{
				Name: "nothing",
			},
		},
	}
	return volume.NewSpecFromVolume(rbdVolume), nil
}

func (plugin *quotaVolumePlugin) RequiresRemount() bool {
	return false
}

func (plugin *quotaVolumePlugin) SupportsMountOption() bool {
	return false
}

func (plugin *quotaVolumePlugin) SupportsBulkVolumeVerification() bool {
	return false
}

// method of PersistentVolumePlugin
func (plugin *quotaVolumePlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadOnlyMany,
	}
}

// Mounter
type quotaVolumeMounter struct{}

var _ volume.Mounter = &quotaVolumeMounter{}

func (plugin *quotaVolumePlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	return &quotaVolumeMounter{}, nil
}

func (b *quotaVolumeMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt("", fsGroup)
}
func (b *quotaVolumeMounter) SetUpAt(dir string, fsGroup *int64) error {
	return nil
}

func (b *quotaVolumeMounter) CanMount() error {
	return nil
}

func (b *quotaVolumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{}
}

func (b *quotaVolumeMounter) GetPath() string {
	return ""
}

func (b *quotaVolumeMounter) GetMetrics() (*volume.Metrics, error) {
	return &volume.Metrics{}, volume.NewNotSupportedError()
}

// UnMounter
type quotaVolumeUnmounter struct{}

var _ volume.Unmounter = &quotaVolumeUnmounter{}

func (plugin *quotaVolumePlugin) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return &quotaVolumeUnmounter{}, nil
}

func (b *quotaVolumeUnmounter) TearDown() error {
	return b.TearDownAt("")
}
func (b *quotaVolumeUnmounter) TearDownAt(dir string) error {
	return nil
}

func (b *quotaVolumeUnmounter) GetPath() string {
	return ""
}

func (b *quotaVolumeUnmounter) GetMetrics() (*volume.Metrics, error) {
	return &volume.Metrics{}, volume.NewNotSupportedError()
}

// Method of DeletableVolumePlugin
type quotaVolumeDeleter struct{}

var _ volume.Deleter = &quotaVolumeDeleter{}

func (plugin *quotaVolumePlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	return &quotaVolumeDeleter{}, nil
}

func (b *quotaVolumeDeleter) GetPath() string {
	return ""
}

func (b *quotaVolumeDeleter) GetMetrics() (*volume.Metrics, error) {
	return &volume.Metrics{}, volume.NewNotSupportedError()
}

func (b *quotaVolumeDeleter) Delete() error {
	return nil
}

// Method of ProvisionableVolumePlugin
type rbdVolumeProvisioner struct {
	options volume.VolumeOptions
}

var _ volume.Provisioner = &rbdVolumeProvisioner{}

func (plugin *quotaVolumePlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	return &rbdVolumeProvisioner{options: options}, nil
}

func (b *rbdVolumeProvisioner) GetPath() string {
	return ""
}

func (b *rbdVolumeProvisioner) GetMetrics() (*volume.Metrics, error) {
	return &volume.Metrics{}, volume.NewNotSupportedError()
}

func (b *rbdVolumeProvisioner) Provision() (*v1.PersistentVolume, error) {
	pv := new(v1.PersistentVolume)
	quotaVolume := new(v1.QuotaVolumeSource)
	quotaVolume.Name = "nothing"
	pv.Spec.PersistentVolumeSource.QuotaVolume = quotaVolume
	pv.Spec.PersistentVolumeReclaimPolicy = b.options.PersistentVolumeReclaimPolicy
	pv.Spec.AccessModes = b.options.PVC.Spec.AccessModes
	if len(pv.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = []v1.PersistentVolumeAccessMode{
			v1.ReadOnlyMany,
		}
	}
	capacity := b.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()
	// convert to MB that rbd defaults on
	sizeMB := int(volume.RoundUpSize(volSizeBytes, 1024*1024))
	pv.Spec.Capacity = v1.ResourceList{
		v1.ResourceName(v1.ResourceStorage): resource.MustParse(fmt.Sprintf("%dMi", sizeMB)),
	}
	pv.Spec.MountOptions = b.options.MountOptions
	return pv, nil
}

// Method of ExpandableVolumePlugin
func (plugin *quotaVolumePlugin) ExpandVolumeDevice(spec *volume.Spec, newSize resource.Quantity, oldSize resource.Quantity) (resource.Quantity, error) {
	return newSize, nil
}

func (plugin *quotaVolumePlugin) RequiresFSResize() bool {
	return false
}
