package com.github.k8soperators.oldman.api.v1alpha;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceSpec;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "metadata", "spec" })
public class CatalogSource {

    @JsonProperty("metadata")
    private ObjectMeta metadata;

    @JsonProperty("spec")
    private CatalogSourceSpec spec;

    public ObjectMeta getMetadata() {
        return metadata;
    }

    public void setMetadata(ObjectMeta metadata) {
        this.metadata = metadata;
    }

    public CatalogSourceSpec getSpec() {
        return spec;
    }

    public void setSpec(CatalogSourceSpec spec) {
        this.spec = spec;
    }

}
