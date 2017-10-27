package fsexpander

import (
	"fmt"
	"time"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/fsexpander/cache"
	actualcache "k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
)

type VolumeFSExpander interface {
	Run(sourcesReady config.SourcesReady, stopCh <-chan struct{})
}

type volumeFSExpander struct {
	pvcInformer         clientcache.SharedIndexInformer
	pvInformer          clientcache.SharedIndexInformer
	loopSleepDuration   time.Duration
	resizeMap           cache.VolumeFSResizeMap
	opsExecutor         operationexecutor.OperationExecutor
	kubeClient          clientset.Interface
	actualStateOfWorld  actualcache.ActualStateOfWorld
	desiredStateOfWorld actualcache.DesiredStateOfWorld
}

func NewVolumeFSExpander(kubeClient clientset.Interface, pvcInformer clientcache.SharedIndexInformer,
	pvInformer clientcache.SharedIndexInformer, volumeExpanderLoopSleepPeriod time.Duration, actualStateOfWorld actualcache.ActualStateOfWorld, desiredStateOfWorld actualcache.DesiredStateOfWorld) VolumeFSExpander {
	expander := &volumeFSExpander{
		loopSleepDuration:   volumeExpanderLoopSleepPeriod,
		pvInformer:          pvInformer,
		kubeClient:          kubeClient,
		resizeMap:           cache.NewVolumeFSResizeMap(kubeClient),
		actualStateOfWorld:  actualStateOfWorld,
		desiredStateOfWorld: desiredStateOfWorld,
	}
	pvcInformer.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
		UpdateFunc: expander.pvcUpdate,
	})
	expander.pvcInformer = pvcInformer

	return expander
}

func (expander *volumeFSExpander) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	// Wait for the completion of a loop that started after sources are all ready, then set hasAddedPods accordingly
	go expander.pvcInformer.Run(stopCh)
	go expander.pvInformer.Run(stopCh)

	wait.PollUntil(expander.loopSleepDuration, func() (bool, error) {
		done := sourcesReady.AllReady()
		expander.expandLoopFunc()()
		return done, nil
	}, stopCh)
	wait.Until(expander.expandLoopFunc(), expander.loopSleepDuration, stopCh)
}

func (expander *volumeFSExpander) expandLoopFunc() func() {
	return func() {
		for _, pvcWithFSResizeRequest := range expander.resizeMap.GetPVCsWithFSResizeRequest() {
			uniqueVolumeKey := v1.UniqueVolumeName(pvcWithFSResizeRequest.UniquePVCKey())
			updatedClaim, err := markPVCFSResizeInProgress(pvcWithFSResizeRequest, expander.kubeClient)
			if err != nil {
				glog.V(5).Infof("Error setting PVC %s in progress with error : %v", pvcWithFSResizeRequest.QualifiedName(), err)
				continue
			}
			if updatedClaim != nil {
				pvcWithFSResizeRequest.PVC = updatedClaim
			}

			if expander.opsExecutor.IsOperationPending(uniqueVolumeKey, "") {
				glog.V(10).Infof("Operation for PVC %v is already pending", pvcWithFSResizeRequest.QualifiedName())
				continue
			}
			glog.V(5).Infof("Starting opsExecutor.ExpandVolumeFS for volume %s", pvcWithFSResizeRequest.QualifiedName())
			growFuncError := expander.opsExecutor.ExpandVolumeFS(pvcWithFSResizeRequest, expander.resizeMap)
			if growFuncError != nil && !exponentialbackoff.IsExponentialBackoff(growFuncError) {
				glog.Errorf("Error growing pvc %s with %v", pvcWithFSResizeRequest.QualifiedName(), growFuncError)
			}
			if growFuncError == nil {
				glog.V(5).Infof("Started opsExecutor.ExpandVolumeFS for volume %s", pvcWithFSResizeRequest.QualifiedName())
			}
		}
	}
}

func (expander *volumeFSExpander) pvcUpdate(oldObj, newObj interface{}) {
	oldPvc, ok := oldObj.(*v1.PersistentVolumeClaim)

	if oldPvc == nil || !ok {
		return
	}

	newPVC, ok := newObj.(*v1.PersistentVolumeClaim)

	if newPVC == nil || !ok {
		return
	}
	for _, mntVol := range expander.actualStateOfWorld.GetMountedVolumes() {
		glog.Errorf("podname(%v) -InnerVolumeSpecName(%v)---OuterVolumeSpecName(%v)---PluginName(%v)-PodUID(%v)--Mnt---VolumeName(%v)-- \n",
			mntVol.PodName,
			mntVol.InnerVolumeSpecName,
			mntVol.OuterVolumeSpecName,
			mntVol.PluginName,
			mntVol.PodUID,
			mntVol.VolumeName)
	}
	for _, mntVol := range expander.desiredStateOfWorld.GetVolumesToMount() {
		glog.Errorf("podname(%v) ----OuterVolumeSpecName(%v)----Mnt---VolumeName(%v)-- \n",
			mntVol.PodName,
			mntVol.OuterVolumeSpecName,
			mntVol.VolumeName)
	}
	return
	if len(oldPvc.Status.Conditions) != len(newPVC.Status.Conditions) {
		glog.Errorf("VM --------PVC----------- %v : %v\n", oldPvc.Status.Conditions, newPVC.Status.Conditions)
	}

	pv, err := getPersistentVolume(newPVC, listerv1.NewPersistentVolumeLister(expander.pvInformer.GetIndexer()))
	if err != nil {
		glog.V(5).Infof("Error getting Persistent Volume for pvc %q : %v", newPVC.UID, err)
		return
	}
	expander.resizeMap.AddPVCUpdate(newPVC, pv)
}

func getPersistentVolume(pvc *v1.PersistentVolumeClaim, pvLister corelisters.PersistentVolumeLister) (*v1.PersistentVolume, error) {
	volumeName := pvc.Spec.VolumeName
	pv, err := pvLister.Get(volumeName)

	if err != nil {
		return nil, fmt.Errorf("failed to find PV %q in PV informer cache with error : %v", volumeName, err)
	}

	return pv.DeepCopy(), nil
}

func markPVCFSResizeInProgress(pvcWithResizeRequest *cache.PVCWithFSResizeRequest, kubeClient clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	// Mark PVC as Resize Started
	progressCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimResizing,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}
	conditions := []v1.PersistentVolumeClaimCondition{progressCondition}

	return cache.UpdatePVCCondition(pvcWithResizeRequest.PVC, conditions, kubeClient)
}
