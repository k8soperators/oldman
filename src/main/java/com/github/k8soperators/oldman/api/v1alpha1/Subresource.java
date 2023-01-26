package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpec;

import javax.validation.constraints.NotNull;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class Subresource<S> {

    static class CatalogSource extends Subresource<CatalogSourceSpec> {
    }

    static class OperatorGroup extends Subresource<OperatorGroupSpec> {
    }

    static class Subscription extends Subresource<SubscriptionSpec> {
    }

    private MetadataPairs metadata;

    @NotNull
    private S spec;

    @JsonIgnore
    Map<String, String> getPairs(Function<MetadataPairs, Map<String, String>> source) {
        return Optional.ofNullable(metadata).map(source::apply).orElseGet(Collections::emptyMap);
    }

    @JsonIgnore
    public Map<String, String> getLabels() {
        return getPairs(MetadataPairs::getLabelsSet);
    }

    @JsonIgnore
    public Map<String, String> getLabelsRemoved() {
        return getPairs(MetadataPairs::getLabelsRemoved);
    }

    @JsonIgnore
    public Map<String, String> getAnnotations() {
        return getPairs(MetadataPairs::getAnnotationsSet);
    }

    @JsonIgnore
    public Map<String, String> getAnnotationsRemoved() {
        return getPairs(MetadataPairs::getAnnotationsRemoved);
    }

    public MetadataPairs getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataPairs metadata) {
        this.metadata = metadata;
    }

    public S getSpec() {
        return spec;
    }

    public void setSpec(S spec) {
        this.spec = spec;
    }

}
