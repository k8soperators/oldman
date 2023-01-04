package com.github.k8soperators.oldman.api.v1alpha;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.SecretKeySelector;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "namespace", "name", "key", "selector" })
public class PropagatedSecret {

    String namespace;
    String name;
    String key;

    SecretKeySelector selector;

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

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public SecretKeySelector getSelector() {
        return selector;
    }

    public void setSelector(SecretKeySelector selector) {
        this.selector = selector;
    }

}
