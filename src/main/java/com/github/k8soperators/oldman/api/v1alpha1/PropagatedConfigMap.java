package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "key", "removed", "source" })
public class PropagatedConfigMap implements PropagatedData<ConfigMap> {

    ConfigMapKeySelector source;

    String name;
    String key;
    boolean removed;

    @Override
    @JsonIgnore
    public Class<ConfigMap> getGenericType() {
        return ConfigMap.class;
    }

    public ConfigMapKeySelector getSource() {
        return source;
    }

    public void setSource(ConfigMapKeySelector source) {
        this.source = source;
    }

    @Override
    @JsonIgnore
    public String getSourceName() {
        return source == null ? null : source.getName();
    }

    @Override
    @JsonIgnore
    public String getSourceKey() {
        return source == null ? null : source.getKey();
    }

    @Override
    @JsonIgnore
    public boolean hasKey(ConfigMap configMap, String sourceKey) {
        if (configMap == null || sourceKey == null) {
            return false;
        }
        return hasKey(configMap.getData(), sourceKey) || hasKey(configMap.getBinaryData(), sourceKey);
    }

    @Override
    @JsonIgnore
    public boolean isSourceOptional() {
        return source == null || Boolean.TRUE.equals(source.getOptional());
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public boolean isRemoved() {
        return removed;
    }

    public void setRemoved(boolean removed) {
        this.removed = removed;
    }
}
