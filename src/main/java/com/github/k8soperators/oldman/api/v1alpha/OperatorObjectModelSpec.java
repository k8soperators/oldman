package com.github.k8soperators.oldman.api.v1alpha;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "configMaps", "secrets", "operators" })
public class OperatorObjectModelSpec {

    List<PropagatedConfigMap> configMaps;

    List<PropagatedSecret> secrets;

    List<OperatorSource> operators;

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

    public List<OperatorSource> getOperators() {
        return operators;
    }

    public void setOperators(List<OperatorSource> operators) {
        this.operators = operators;
    }

}
