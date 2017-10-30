package fsexpander

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/kubelet/config"
	actualcache "k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/fsexpander/cache"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/client-go/tools/record"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kubernetes/pkg/volume"
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
	pvcPopulator        PVCPopulator
}

func NewVolumeFSExpander(kubeClient clientset.Interface, pvcInformer clientcache.SharedIndexInformer,
	pvInformer clientcache.SharedIndexInformer, volumeExpanderLoopSleepPeriod time.Duration, volumeExpanderPvcPopulatorLoopSleepPeriod time.Duration,
	actualStateOfWorld actualcache.ActualStateOfWorld, desiredStateOfWorld actualcache.DesiredStateOfWorld, volumePluginMgr *volume.VolumePluginMgr) VolumeFSExpander {

	resizeMap := cache.NewVolumeFSResizeMap(kubeClient)

	populator := NewPVCPopulator(
		volumeExpanderPvcPopulatorLoopSleepPeriod,
		resizeMap,
		corelisters.NewPersistentVolumeClaimLister(pvcInformer.GetIndexer()),
		corelisters.NewPersistentVolumeLister(pvInformer.GetIndexer()),
		kubeClient,
	)

	expander := &volumeFSExpander{
		loopSleepDuration:   volumeExpanderLoopSleepPeriod,
		pvInformer:          pvInformer,
		kubeClient:          kubeClient,
		resizeMap:           resizeMap,
		actualStateOfWorld:  actualStateOfWorld,
		desiredStateOfWorld: desiredStateOfWorld,
		pvcPopulator:        populator,
	}
	pvcInformer.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
		UpdateFunc: expander.pvcUpdate,
	})
	expander.pvcInformer = pvcInformer

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "volume_fs_expand"})

	expander.opsExecutor = operationexecutor.NewOperationExecutor(operationexecutor.NewOperationGenerator(
		kubeClient,
		volumePluginMgr,
		recorder,
		false))

	return expander
}

func (expander *volumeFSExpander) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	// Wait for the completion of a loop that started after sources are all ready, then set hasAddedPods accordingly
	go expander.pvcInformer.Run(stopCh)
	go expander.pvInformer.Run(stopCh)
	go expander.pvcPopulator.Run(stopCh)
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
			desiredPvcNames := make(map[string]string)
			actualPvcNames := make(map[string]string)
			desiredPvcs := expander.desiredStateOfWorld.GetVolumesToMount()
			for _, v := range desiredPvcs {
				desiredPvcNames[v.OuterVolumeSpecName] = ""
			}
			if _, exist := desiredPvcNames[pvcWithFSResizeRequest.PVC.Name]; !exist {
				continue
			}

			actualPvcs := expander.actualStateOfWorld.GetMountedVolumes()
			for _, v := range actualPvcs {
				actualPvcNames[v.OuterVolumeSpecName] = ""
			}
			if _, exist := actualPvcNames[pvcWithFSResizeRequest.PVC.Name]; !exist {
				continue
			}

			uniqueVolumeKey := v1.UniqueVolumeName(pvcWithFSResizeRequest.UniquePVCKey())
			updatedClaim, err := markPVCFSResizeInProgress(pvcWithFSResizeRequest, expander.kubeClient)
			if err != nil {
				glog.V(5).Infof("fsexpander: Error setting PVC %s in progress with error : %v", pvcWithFSResizeRequest.QualifiedName(), err)
				continue
			}
			if updatedClaim != nil {
				pvcWithFSResizeRequest.PVC = updatedClaim
			}

			if expander.opsExecutor.IsOperationPending(uniqueVolumeKey, "") {
				glog.V(10).Infof("fsexpander: Operation for PVC %v is already pending", pvcWithFSResizeRequest.QualifiedName())
				continue
			}
			glog.V(5).Infof("fsexpander: Starting opsExecutor.ExpandVolumeFS for volume %s", pvcWithFSResizeRequest.QualifiedName())
			growFuncError := expander.opsExecutor.ExpandVolumeFS(pvcWithFSResizeRequest, expander.resizeMap)
			if growFuncError != nil && !exponentialbackoff.IsExponentialBackoff(growFuncError) {
				glog.Errorf("fsexpander: Error growing pvc %s with %v", pvcWithFSResizeRequest.QualifiedName(), growFuncError)
			}
			if growFuncError == nil {
				glog.V(5).Infof("fsexpander: Started opsExecutor.ExpandVolumeFS for volume %s", pvcWithFSResizeRequest.QualifiedName())
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

	// just deal with in-use pvc
	status, exists := newPVC.Labels["status"]
	if !exists || (exists && status != "using") {
		return
	}

	pv, err := getPersistentVolume(newPVC, listerv1.NewPersistentVolumeLister(expander.pvInformer.GetIndexer()))
	if err != nil {
		glog.V(5).Infof("fsexpander: Error getting Persistent Volume for pvc %q : %v", newPVC.UID, err)
		return
	}
	expander.resizeMap.AddPVCUpdate(newPVC, pv)
}

func getPersistentVolume(pvc *v1.PersistentVolumeClaim, pvLister corelisters.PersistentVolumeLister) (*v1.PersistentVolume, error) {
	volumeName := pvc.Spec.VolumeName
	pv, err := pvLister.Get(volumeName)

	if err != nil {
		return nil, fmt.Errorf("fsexpander: failed to find PV %q in PV informer cache with error : %v", volumeName, err)
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
