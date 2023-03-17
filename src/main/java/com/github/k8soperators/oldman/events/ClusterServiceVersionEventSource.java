package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Store;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.ClusterServiceVersion;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.jboss.logging.Logger;

import java.util.Objects;
import java.util.stream.Stream;

public class ClusterServiceVersionEventSource extends AbstractEventSource implements ResourceEventHandler<ClusterServiceVersion> {

    private static final Logger log = Logger.getLogger(ClusterServiceVersionEventSource.class);

    final IndexerResourceCache<OperatorObjectModel> primaryCache;
    final OwnedResourceEventSource ownedResources;
    final SharedIndexInformer<ClusterServiceVersion> informer;

    public ClusterServiceVersionEventSource(
            IndexerResourceCache<OperatorObjectModel> primaryCache,
            OwnedResourceEventSource ownedResources,
            KubernetesClient client) {
        this.primaryCache = primaryCache;
        this.ownedResources = ownedResources;

        informer = client.resources(ClusterServiceVersion.class)
                .inAnyNamespace()
                .withoutLabel("olm.copiedFrom")
                .inform();
        informer.addEventHandler(this);
    }

    public Store<ClusterServiceVersion> getStore() {
        return informer.getStore();
    }

    public ClusterServiceVersion get(String key) {
        return getStore().getByKey(key);
    }

    public ClusterServiceVersion get(String namespace, String name) {
        return get(Cache.namespaceKeyFunc(namespace, name));
    }

    @Override
    public void onAdd(ClusterServiceVersion obj) {
        log.debugf("%s{namespace=%s, name=%s}: added", obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
        getOwners(obj).forEach(this::handleEvent);
    }

    @Override
    public void onUpdate(ClusterServiceVersion oldObj, ClusterServiceVersion obj) {
        log.debugf("%s{namespace=%s, name=%s}: updated", obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
        getOwners(obj).forEach(this::handleEvent);
    }

    @Override
    public void onDelete(ClusterServiceVersion obj, boolean deletedFinalStateUnknown) {
        log.debugf("%s{namespace=%s, name=%s}: deleted", obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
        getOwners(obj).forEach(this::handleEvent);
    }

    Stream<OperatorObjectModel> getOwners(ClusterServiceVersion obj) {
        Namespace namespace = ownedResources.get(Namespace.class, obj.getMetadata().getNamespace());

        if (namespace == null) {
            return Stream.empty();
        }

        return ownedResources.getInformer(Subscription.class)
            .getIndexer()
            .byIndex(Cache.NAMESPACE_INDEX, namespace.getMetadata().getName())
            .stream()
            .filter(sub -> Objects.nonNull(sub.getStatus()))
            .filter(sub -> Objects.equals(obj.getMetadata().getName(), sub.getStatus().getInstalledCSV()))
            .flatMap(sub -> primaryCache.list().filter(model -> model.isControllingOwner(sub)));
    }

    void handleEvent(OperatorObjectModel model) {
        getEventHandler()
            .handleEvent(new ResourceEvent(ResourceAction.UPDATED, ResourceID.fromResource(model), model));
    }
}
