package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ObjectReference;

import java.util.List;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "namespace",
        "name",
        "configMaps",
        "secrets",
        "operatorGroup",
        "catalogSource",
        "subscription",
        "cleanupResources" })
public class OperatorSource {

    private String namespace;

    private String name;

    private List<PropagatedConfigMap> configMaps;

    private List<PropagatedSecret> secrets;

    private Subresource.OperatorGroup operatorGroup;

    private Subresource.CatalogSource catalogSource;

    private Subresource.Subscription subscription;

    private List<ObjectReference> cleanupResources;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<PropagatedConfigMap> getConfigMaps() {
        return configMaps;
    }

    public void setConfigMaps(List<PropagatedConfigMap> configMaps) {
        this.configMaps = configMaps;
    }

    public List<PropagatedSecret> getSecrets() {
        return secrets;
    }

    public void setSecrets(List<PropagatedSecret> secrets) {
        this.secrets = secrets;
    }

    public Subresource.OperatorGroup getOperatorGroup() {
        return operatorGroup;
    }

    public void setOperatorGroup(Subresource.OperatorGroup operatorGroup) {
        this.operatorGroup = operatorGroup;
    }

    public Subresource.CatalogSource getCatalogSource() {
        return catalogSource;
    }

    public void setCatalogSource(Subresource.CatalogSource catalogSource) {
        this.catalogSource = catalogSource;
    }

    public Subresource.Subscription getSubscription() {
        return subscription;
    }

    public void setSubscription(Subresource.Subscription subscription) {
        this.subscription = subscription;
    }

    public List<ObjectReference> getCleanupResources() {
        return cleanupResources;
    }

    public void setCleanupResources(List<ObjectReference> removeObjects) {
        this.cleanupResources = removeObjects;
    }

}
