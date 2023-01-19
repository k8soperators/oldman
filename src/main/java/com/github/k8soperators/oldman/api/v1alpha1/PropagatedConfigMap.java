package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "namespace", "name", "key", "source" })
public class PropagatedConfigMap implements PropagatedData {

    ConfigMapKeySelector source;

    String name;
    String key;

    public ConfigMapKeySelector getSource() {
        return source;
    }

    public void setSource(ConfigMapKeySelector source) {
        this.source = source;
    }

    @Override
    @JsonIgnore
    public String getSourceName() {
        return source.getName();
    }

    @Override
    @JsonIgnore
    public String getSourceKey() {
        return source.getKey();
    }

    @Override
    @JsonIgnore
    public boolean isSourceOptional() {
        return Boolean.TRUE.equals(source.getOptional());
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

}
