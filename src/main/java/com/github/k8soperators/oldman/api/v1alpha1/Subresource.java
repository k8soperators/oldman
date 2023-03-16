package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpec;

import javax.validation.constraints.NotNull;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public abstract class Subresource<K extends HasMetadata, S> {

    private MetadataPairs metadata;

    @NotNull
    private S spec;

    @JsonIgnore
    protected abstract S getSpec(K resource);

    @JsonIgnore
    public boolean hasDesiredState(K actual, K desired) {
        if (!hasDesiredState(actual.getMetadata().getAnnotations(), getAnnotations(), getAnnotationsRemoved())) {
            return false;
        }
        if (!hasDesiredState(actual.getMetadata().getLabels(), getLabels(), getLabelsRemoved())) {
            return false;
        }

        return Objects.equals(getSpec(actual), getSpec(desired));
    }

    @JsonIgnore
    static boolean containsPair(Map<String, String> pairs, Map.Entry<String, String> pair) {
        return pairs.containsKey(pair.getKey()) && Objects.equals(pairs.get(pair.getKey()), pair.getValue());
    }

    @JsonIgnore
    boolean hasDesiredState(Map<String, String> actualPairs, Map<String, String> addedPairs, Map<String, String> removedPairs) {
        return Optional.ofNullable(actualPairs)
            .map(pairs ->
                addedPairs.entrySet().stream().allMatch(e -> containsPair(pairs, e)) &&
                removedPairs.entrySet().stream().noneMatch(e -> pairs.containsKey(e.getKey())))
            .orElseGet(addedPairs::isEmpty);
    }

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

    static class CatalogSource extends Subresource<io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSource, CatalogSourceSpec> {
        static final String KIND = "CatalogSource";

        @Override
        protected CatalogSourceSpec getSpec(io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSource resource) {
            return resource.getSpec();
        }
    }

    static class OperatorGroup extends Subresource<io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroup, OperatorGroupSpec> {
        static final String KIND = "OperatorGroup";

        @Override
        protected OperatorGroupSpec getSpec(io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroup resource) {
            return resource.getSpec();
        }
    }

    static class Subscription extends Subresource<io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription, SubscriptionSpec> {
        static final String KIND = "Subscription";

        @Override
        protected SubscriptionSpec getSpec(io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription resource) {
            return resource.getSpec();
        }
    }

    @SuppressWarnings("unchecked")
    public static <K extends HasMetadata, S> Subresource<K, S> newInstance(String kind) {
        switch (kind) {
        case CatalogSource.KIND:
            return (Subresource<K, S>) new CatalogSource();
        case OperatorGroup.KIND:
            return (Subresource<K, S>) new OperatorGroup();
        case Subscription.KIND:
            return (Subresource<K, S>) new Subscription();
        default:
            throw new IllegalArgumentException(String.valueOf(kind));
        }
    }
}
