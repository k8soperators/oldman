package com.github.k8soperators.oldman.api.v1alpha1;

public interface PropagatedData {

    String getSourceName();

    String getSourceKey();

    boolean isSourceOptional();

    String getName();

    String getKey();

}
