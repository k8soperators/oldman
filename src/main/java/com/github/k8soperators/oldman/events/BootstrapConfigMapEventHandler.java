package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Optional;

import static com.github.k8soperators.oldman.OperatorObjectModelReconciler.LABEL_KEY_MANAGED_BY;
import static com.github.k8soperators.oldman.OperatorObjectModelReconciler.OLDMAN;

public class BootstrapConfigMapEventHandler implements ResourceEventHandler<ConfigMap> {

    private static final Logger log = Logger.getLogger(BootstrapConfigMapEventHandler.class);

    private final KubernetesClient client;

    public BootstrapConfigMapEventHandler(KubernetesClient client) {
        this.client = client;
    }

    @Override
    public void onAdd(ConfigMap bootstrap) {
        log.infof("Bootstrap ConfigMap added", bootstrap);
        client.resource(toModel(bootstrap)).createOrReplace();
    }

    @Override
    public void onUpdate(ConfigMap oldBootstrap, ConfigMap newBootstrap) {
        log.infof("Bootstrap ConfigMap updated", newBootstrap);
        client.resource(toModel(newBootstrap)).createOrReplace();
    }

    @Override
    public void onDelete(ConfigMap deletedBootstrap, boolean deletedFinalStateUnknown) {
        // Do nothing, leaving any previously-created bootstrap CR in place
        log.infof("Bootstrap ConfigMap deleted", deletedBootstrap);

        try {
            client.resource(toModel(deletedBootstrap))
                .edit(model -> {
                    Optional.ofNullable(model.getMetadata().getLabels())
                        .filter(labels -> OLDMAN.equals(labels.get(LABEL_KEY_MANAGED_BY)))
                        .ifPresent(labels -> labels.remove(LABEL_KEY_MANAGED_BY));

                    return model;
                });
        } catch (KubernetesClientException e) {
            if (e.getCode() != 404) {
                throw e;
            }
        }
    }

    OperatorObjectModel toModel(ConfigMap bootstrap) {
        String serializedModel = bootstrap.getData().get("model");
        OperatorObjectModel model = Serialization.unmarshal(serializedModel, OperatorObjectModel.class);

        Optional.ofNullable(model.getMetadata().getLabels())
            .ifPresentOrElse(this::addManagedByLabel, () -> initializeLabels(model));

        return model;
    }

    void addManagedByLabel(Map<String, String> labels) {
        labels.put(LABEL_KEY_MANAGED_BY, OLDMAN);
    }

    void initializeLabels(OperatorObjectModel model) {
        model.getMetadata().setLabels(Map.of(LABEL_KEY_MANAGED_BY, OLDMAN));
    }
}
