package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretKeySelector;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name", "key", "type", "removed", "source" })
public class PropagatedSecret implements PropagatedData<Secret> {

    SecretKeySelector source;

    String name;
    String key;
    String type;
    boolean removed;

    @Override
    @JsonIgnore
    public Class<Secret> getGenericType() {
        return Secret.class;
    }

    public SecretKeySelector getSource() {
        return source;
    }

    public void setSource(SecretKeySelector selector) {
        this.source = selector;
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
    public boolean isSourceMissing(Secret secret) {
        if (secret == null) {
            return true;
        }

        String sourceKey = getSourceKey();

        if (sourceKey == null) {
            return false;
        }

        return !hasKey(secret.getData(), sourceKey) && !hasKey(secret.getStringData(), sourceKey);
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean isRemoved() {
        return removed;
    }

    public void setRemoved(boolean removed) {
        this.removed = removed;
    }

}
