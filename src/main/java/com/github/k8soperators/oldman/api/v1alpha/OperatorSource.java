package com.github.k8soperators.oldman.api.v1alpha;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "catalogSource", "subscription" })
public class OperatorSource {

    CatalogSource catalogSource;
    Subscription subscription;
    List<OperatorDependency> dependencies;

    public CatalogSource getCatalogSource() {
        return catalogSource;
    }

    public void setCatalogSource(CatalogSource catalogSource) {
        this.catalogSource = catalogSource;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }

    public List<OperatorDependency> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<OperatorDependency> dependencies) {
        this.dependencies = dependencies;
    }

}
