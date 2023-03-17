package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

import java.util.Collection;

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

    @JsonIgnore
    public void own(HasMetadata resource) {
        int ownerCount = resource.optionalMetadata()
                .map(ObjectMeta::getOwnerReferences)
                .map(Collection::size)
                .orElse(0);

        resource.getOwnerReferenceFor(this)
            .ifPresentOrElse(
                    or -> {
                        or.setApiVersion(getApiVersion());
                        or.setKind(getKind());
                        or.setName(getMetadata().getName());
                        or.setUid(getMetadata().getUid());
                        or.setController(ownerCount == 1 || Boolean.TRUE.equals(or.getController()));
                    },
                    () ->
                        resource.addOwnerReference(new OwnerReferenceBuilder()
                            .withApiVersion(getApiVersion())
                            .withKind(getKind())
                            .withName(getMetadata().getName())
                            .withUid(getMetadata().getUid())
                            .withController(ownerCount == 0)
                            .build()));
    }

    @JsonIgnore
    public boolean isControllingOwner(HasMetadata resource) {
        return resource.getOwnerReferenceFor(this)
            .map(OwnerReference::getController)
            .filter(Boolean.TRUE::equals)
            .orElse(false);
    }
}
