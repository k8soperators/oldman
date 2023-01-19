package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpec;

import java.util.List;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "namespace", "name", "configMaps", "secrets", "operatorGroupSpec", "catalogSourceSpec", "subscriptionSpec" })
public class OperatorSource {

    private String namespace;

    private String name;

    private List<PropagatedConfigMap> configMaps;

    private List<PropagatedSecret> secrets;

    @JsonProperty("operatorGroupSpec")
    private OperatorGroupSpec operatorGroupSpec;

    @JsonProperty("catalogSourceSpec")
    private CatalogSourceSpec catalogSourceSpec;

    @JsonProperty("subscriptionSpec")
    private SubscriptionSpec subscriptionSpec;

    private List<ObjectReference> removeObjects;

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

    public OperatorGroupSpec getOperatorGroupSpec() {
        return operatorGroupSpec;
    }

    public void setOperatorGroupSpec(OperatorGroupSpec operatorGroupSpec) {
        this.operatorGroupSpec = operatorGroupSpec;
    }

    public CatalogSourceSpec getCatalogSourceSpec() {
        return catalogSourceSpec;
    }

    public void setCatalogSourceSpec(CatalogSourceSpec catalogSourceSpec) {
        this.catalogSourceSpec = catalogSourceSpec;
    }

    public SubscriptionSpec getSubscriptionSpec() {
        return subscriptionSpec;
    }

    public void setSubscriptionSpec(SubscriptionSpec subscriptionSpec) {
        this.subscriptionSpec = subscriptionSpec;
    }

    public List<ObjectReference> getRemoveObjects() {
        return removeObjects;
    }

    public void setRemoveObjects(List<ObjectReference> removeObjects) {
        this.removeObjects = removeObjects;
    }

}
