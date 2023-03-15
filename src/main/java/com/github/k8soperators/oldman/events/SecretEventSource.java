package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorSource;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedSecret;
import io.fabric8.kubernetes.api.model.Secret;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;

import java.util.List;
import java.util.function.Function;

public class SecretEventSource extends ConfigurationEventSource<Secret, PropagatedSecret> {

    public SecretEventSource(IndexerResourceCache<OperatorObjectModel> primaryCache) {
        super(primaryCache);
    }

    @Override
    Function<OperatorSource, List<PropagatedSecret>> getDataSource() {
        return OperatorSource::getSecrets;
    }
}
