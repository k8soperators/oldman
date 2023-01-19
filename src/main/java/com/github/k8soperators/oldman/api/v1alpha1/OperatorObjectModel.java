package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("oldman.k8soperators.github.com")
public class OperatorObjectModel extends CustomResource<OperatorObjectModelSpec, OperatorObjectModelStatus> {

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    public OperatorObjectModelStatus getOrCreateStatus() {
        if (status == null) {
            status = new OperatorObjectModelStatus();
        }

        return status;
    }
}
