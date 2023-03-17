package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorSource;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedData;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.jboss.logging.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

abstract class ConfigurationEventSource<T extends HasMetadata, P extends PropagatedData<T>> extends AbstractEventSource implements ResourceEventHandler<T> {

    private final Logger log = Logger.getLogger(getClass());

    protected final IndexerResourceCache<OperatorObjectModel> primaryCache;

    protected ConfigurationEventSource(IndexerResourceCache<OperatorObjectModel> primaryCache) {
        this.primaryCache = primaryCache;
    }

    @Override
    public void onAdd(T obj) {
        log.debugf("%s{namespace=%s, name=%s}: added", obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
        getReferencingObjectModels(obj).forEach(this::handleEvent);
    }

    @Override
    public void onUpdate(T oldObj, T obj) {
        log.debugf("%s{namespace=%s, name=%s}: updated", obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
        getReferencingObjectModels(obj).forEach(this::handleEvent);
    }

    @Override
    public void onDelete(T obj, boolean deletedFinalStateUnknown) {
        log.debugf("%s{namespace=%s, name=%s}: deleted", obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
        getReferencingObjectModels(obj).forEach(this::handleEvent);
    }

    Stream<OperatorObjectModel> getReferencingObjectModels(T obj) {
        return primaryCache.list().filter(model -> isReferenced(model, obj));
    }

    void handleEvent(OperatorObjectModel model) {
        getEventHandler().handleEvent(new ResourceEvent(ResourceAction.UPDATED, ResourceID.fromResource(model), model));
    }

    boolean isReferenced(OperatorObjectModel model, T dataSource) {
        return model.getSpec()
                .getOperators()
                .stream()
                .map(getDataSource())
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(PropagatedData::getSourceName)
                .map(name -> dataSource.getMetadata().getName().equals(name))
                .findFirst()
                .orElse(false);
    }

    abstract Function<OperatorSource, List<P>> getDataSource();
}
