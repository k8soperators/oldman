package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

@JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "operators" })
public class OperatorObjectModelSpec {

    List<OperatorSource> operators;

    public List<OperatorSource> getOperators() {
        return operators;
    }

    public void setOperators(List<OperatorSource> operators) {
        this.operators = operators;
    }

}
