package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectReference;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "subscriptionRef", "healthy", "lastUpdated" })
public class SubscriptionHealth implements KubernetesResource {

    private static final long serialVersionUID = 1L;

    @JsonProperty("subscriptionRef")
    private ObjectReference subscriptionRef;
    @JsonProperty("healthy")
    private Boolean healthy;
    @JsonProperty("lastUpdated")
    private String lastUpdated;

    @JsonProperty("subscriptionRef")
    public ObjectReference getSubscriptionRef() {
        return subscriptionRef;
    }

    @JsonProperty("subscriptionRef")
    public void setSubscriptionRef(ObjectReference subscriptionRef) {
        this.subscriptionRef = subscriptionRef;
    }

    @JsonProperty("healthy")
    public Boolean getHealthy() {
        return healthy;
    }

    @JsonProperty("healthy")
    public void setHealthy(Boolean healthy) {
        this.healthy = healthy;
    }

    @JsonProperty("lastUpdated")
    public String getLastUpdated() {
        return lastUpdated;
    }

    @JsonProperty("lastUpdated")
    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

}
