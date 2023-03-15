package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorSource;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;

import java.util.List;
import java.util.function.Function;

public class ConfigMapEventSource extends ConfigurationEventSource<ConfigMap, PropagatedConfigMap> {

    public ConfigMapEventSource(IndexerResourceCache<OperatorObjectModel> primaryCache) {
        super(primaryCache);
    }

    @Override
    Function<OperatorSource, List<PropagatedConfigMap>> getDataSource() {
        return OperatorSource::getConfigMaps;
    }
}
