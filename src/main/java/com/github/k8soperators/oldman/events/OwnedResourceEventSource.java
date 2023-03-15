package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.OperatorObjectModelReconciler;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Store;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class OwnedResourceEventSource extends AbstractEventSource implements ResourceEventHandler<HasMetadata> {
    final IndexerResourceCache<OperatorObjectModel> primaryCache;
    final Map<Class<? extends HasMetadata>, SharedIndexInformer<? extends HasMetadata>> informers = new HashMap<>();

    public OwnedResourceEventSource(IndexerResourceCache<OperatorObjectModel> primaryCache) {
        this.primaryCache = primaryCache;
    }

    public void addInformer(Class<? extends HasMetadata> type, KubernetesClient client) {
        addInformer(type, client.resources(type)
                    .inAnyNamespace()
                    .withLabel(OperatorObjectModelReconciler.LABEL_KEY_MANAGED_BY, OperatorObjectModelReconciler.OLDMAN)
                    .inform());
    }

    public void addInformer(Class<? extends HasMetadata> type, SharedIndexInformer<? extends HasMetadata> informer) {
        informer.addEventHandler(this);
        informers.put(type, informer);
    }

    @SuppressWarnings("unchecked")
    public <T extends HasMetadata> SharedIndexInformer<T> getInformer(Class<T> type) {
        return (SharedIndexInformer<T>) informers.get(type);
    }

    public <T extends HasMetadata> Store<T> getStore(Class<T> type) {
        return getInformer(type).getStore();
    }

    public <T extends HasMetadata> T get(Class<T> type, String key) {
        return getInformer(type).getStore().getByKey(key);
    }

    public <T extends HasMetadata> T get(Class<T> type, String namespace, String name) {
        return get(type, Cache.namespaceKeyFunc(namespace, name));
    }

    @Override
    public void onAdd(HasMetadata obj) {
        getOwners(obj).forEach(this::handleEvent);
    }

    @Override
    public void onUpdate(HasMetadata oldObj, HasMetadata newObj) {
        getOwners(newObj).forEach(this::handleEvent);
    }

    @Override
    public void onDelete(HasMetadata obj, boolean deletedFinalStateUnknown) {
        getOwners(obj).forEach(this::handleEvent);
    }

    Stream<OperatorObjectModel> getOwners(HasMetadata obj) {
        return primaryCache.list().filter(model -> isControllingOwner(model, obj));
    }

    boolean isControllingOwner(OperatorObjectModel model, HasMetadata obj) {
        return obj.getOwnerReferenceFor(model)
                .map(OwnerReference::getController)
                .map(Boolean.TRUE::equals)
                .orElse(false);
    }

    void handleEvent(OperatorObjectModel model) {
        getEventHandler()
            .handleEvent(new ResourceEvent(ResourceAction.UPDATED, ResourceID.fromResource(model), model));
    }
}
