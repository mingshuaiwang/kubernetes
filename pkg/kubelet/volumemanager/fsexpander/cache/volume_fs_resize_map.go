package cache

import (
	"fmt"
	"sync"
	"encoding/json"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	commontypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/controller/volume/expand/util"
)

// VolumeResizeMap defines an interface that serves as a cache for holding pending resizing requests
type VolumeFSResizeMap interface {
	// AddPVCUpdate adds pvc for resizing
	AddPVCUpdate(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume)
	//// DeletePVC deletes pvc that is scheduled for resizing
	//DeletePVC(pvc *v1.PersistentVolumeClaim)
	// GetPVCsWithFSResizeRequest returns all pending pvc resize requests
	GetPVCsWithFSResizeRequest() []*PVCWithFSResizeRequest
	// MarkAsResized marks a pvc as fully resized
	MarkAsResized(*PVCWithFSResizeRequest, resource.Quantity) error
	// UpdatePVSize updates just pv size after cloudprovider resizing is successful
	UpdatePVSize(*PVCWithFSResizeRequest, resource.Quantity) error
}

type volumeFSResizeMap struct {
	// map of unique pvc name and resize requests that are pending or inflight
	pvcrs      map[types.UniquePVCName]*PVCWithFSResizeRequest
	kubeClient clientset.Interface
	// for guarding access to pvcrs map
	sync.RWMutex
}

// PVCWithFSResizeRequest struct defines data structure that stores state needed for
// performing file system resize
type PVCWithFSResizeRequest struct {
	// PVC that needs to be resized
	PVC *v1.PersistentVolumeClaim
	// persistentvolume
	PersistentVolume *v1.PersistentVolume
	// Current volume size
	CurrentSize resource.Quantity
	// Expended volume size
	ExpectedSize resource.Quantity
}

func NewVolumeFSResizeMap(kubeClient clientset.Interface) VolumeFSResizeMap {
	return &volumeFSResizeMap{
		kubeClient: kubeClient,
		pvcrs:      make(map[types.UniquePVCName]*PVCWithFSResizeRequest),
	}
}
func (resizeMap *volumeFSResizeMap) AddPVCUpdate(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) {
	if pv.Spec.ClaimRef == nil || pvc.Namespace != pv.Spec.ClaimRef.Namespace || pvc.Name != pv.Spec.ClaimRef.Name {
		glog.V(4).Infof("Persistent Volume is not bound to PVC being updated : %s", util.ClaimToClaimKey(pvc))
		return
	}

	if pvc.Status.Phase != v1.ClaimBound {
		return
	}

	resizeMap.Lock()
	defer resizeMap.Unlock()

	pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcStatusSize := pvc.Status.Capacity[v1.ResourceStorage]

	if pvcStatusSize.Cmp(pvcSize) >= 0 {
		return
	}
	pvcRequest := &PVCWithFSResizeRequest{
		PVC:              pvc,
		CurrentSize:      pvcStatusSize,
		ExpectedSize:     pvcSize,
		PersistentVolume: pv,
	}
	resizeMap.pvcrs[types.UniquePVCName(pvc.UID)] = pvcRequest
}

// GetPVCsWithFSResizeRequest returns all pending pvc resize requests
func (resizeMap *volumeFSResizeMap) GetPVCsWithFSResizeRequest() []*PVCWithFSResizeRequest {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	pvcrs := []*PVCWithFSResizeRequest{}
	for _, pvcr := range resizeMap.pvcrs {
		pvcrs = append(pvcrs, pvcr)
	}
	// Empty out pvcrs map, we will add back failed resize requests later
	resizeMap.pvcrs = map[types.UniquePVCName]*PVCWithFSResizeRequest{}
	return pvcrs
}

// MarkAsResized marks a pvc as fully resized(both volume and fs resized)
func (resizeMap *volumeFSResizeMap) MarkAsResized(pvcr *PVCWithFSResizeRequest, newSize resource.Quantity) error {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	emptyCondition := []v1.PersistentVolumeClaimCondition{}

	err := resizeMap.UpdatePVCCapacityAndConditions(pvcr, newSize, emptyCondition)
	if err != nil {
		glog.V(4).Infof("fsexpander: Error updating PV spec capacity for volume %q with : %v", pvcr.QualifiedName(), err)
		return err
	}
	return nil
}

func (resizeMap *volumeFSResizeMap) UpdatePVCCapacityAndConditions(pvcr *PVCWithFSResizeRequest, newSize resource.Quantity, pvcConditions []v1.PersistentVolumeClaimCondition) error {

	claimClone := pvcr.PVC.DeepCopy()

	claimClone.Status.Capacity[v1.ResourceStorage] = newSize
	claimClone.Status.Conditions = pvcConditions

	_, updateErr := resizeMap.kubeClient.CoreV1().PersistentVolumeClaims(claimClone.Namespace).UpdateStatus(claimClone)
	if updateErr != nil {
		glog.V(4).Infof("fsexpander: updating PersistentVolumeClaim[%s] status: failed: %v", pvcr.QualifiedName(), updateErr)
		return updateErr
	}
	return nil
}

// UpdatePVSize updates just pv size after volume resizing is successful
func (resizeMap *volumeFSResizeMap) UpdatePVSize(pvcr *PVCWithFSResizeRequest, newSize resource.Quantity) error {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	oldPv := pvcr.PersistentVolume
	pvClone := oldPv.DeepCopy()

	oldData, err := json.Marshal(pvClone)

	if err != nil {
		return fmt.Errorf("fsexpander: Unexpected error marshaling PV : %q with error %v", pvClone.Name, err)
	}

	pvClone.Spec.Capacity[v1.ResourceStorage] = newSize

	newData, err := json.Marshal(pvClone)

	if err != nil {
		return fmt.Errorf("fsexpander: Unexpected error marshaling PV : %q with error %v", pvClone.Name, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, pvClone)

	if err != nil {
		return fmt.Errorf("fsexpander: Error Creating two way merge patch for  PV : %q with error %v", pvClone.Name, err)
	}

	_, updateErr := resizeMap.kubeClient.CoreV1().PersistentVolumes().Patch(pvClone.Name, commontypes.StrategicMergePatchType, patchBytes)

	if updateErr != nil {
		glog.V(4).Infof("fsexpander: Error updating pv %q with error : %v", pvClone.Name, updateErr)
		return updateErr
	}
	return nil
}

// UniquePVCKey returns unique key of the PVC based on its UID
func (pvcr *PVCWithFSResizeRequest) UniquePVCKey() types.UniquePVCName {
	return types.UniquePVCName(pvcr.PVC.UID)
}

// QualifiedName returns namespace and name combination of the PVC
func (pvcr *PVCWithFSResizeRequest) QualifiedName() string {
	return strings.JoinQualifiedName(pvcr.PVC.Namespace, pvcr.PVC.Name)
}

// UpdatePVCCondition updates pvc with given condition status
func UpdatePVCCondition(pvc *v1.PersistentVolumeClaim,
	pvcConditions []v1.PersistentVolumeClaimCondition,
	kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {

	claimClone := pvc.DeepCopy()
	claimClone.Status.Conditions = pvcConditions
	updatedClaim, updateErr := kubeClient.CoreV1().PersistentVolumeClaims(claimClone.Namespace).UpdateStatus(claimClone)
	if updateErr != nil {
		glog.V(4).Infof("fsexpander: updating PersistentVolumeClaim[%s] status: failed: %v", ClaimToClaimKey(pvc), updateErr)
		return nil, updateErr
	}
	return updatedClaim, nil
}

// ClaimToClaimKey return namespace/name string for pvc
func ClaimToClaimKey(claim *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", claim.Namespace, claim.Name)
}
