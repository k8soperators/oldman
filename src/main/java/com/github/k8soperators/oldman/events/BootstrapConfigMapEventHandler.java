package com.github.k8soperators.oldman.events;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import org.jboss.logging.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.github.k8soperators.oldman.OperatorObjectModelReconciler.LABEL_KEY_MANAGED_BY;
import static com.github.k8soperators.oldman.OperatorObjectModelReconciler.OLDMAN;

public class BootstrapConfigMapEventHandler implements ResourceEventHandler<ConfigMap> {

    private static final Logger log = Logger.getLogger(BootstrapConfigMapEventHandler.class);

    private final KubernetesClient client;
    private final IndexerResourceCache<OperatorObjectModel> primaryCache;

    public BootstrapConfigMapEventHandler(KubernetesClient client, IndexerResourceCache<OperatorObjectModel> primaryCache) {
        this.client = client;
        this.primaryCache = primaryCache;
    }

    @Override
    public void onAdd(ConfigMap bootstrap) {
        log.infof("Bootstrap ConfigMap{namespace=%s, name=%s}: added", bootstrap.getMetadata().getNamespace(), bootstrap.getMetadata().getName());
        createOrReplaceModel(bootstrap);
    }

    @Override
    public void onUpdate(ConfigMap oldBootstrap, ConfigMap bootstrap) {
        log.infof("Bootstrap ConfigMap{namespace=%s, name=%s}: updated", bootstrap.getMetadata().getNamespace(), bootstrap.getMetadata().getName());
        createOrReplaceModel(bootstrap);
    }

    @Override
    public void onDelete(ConfigMap bootstrap, boolean deletedFinalStateUnknown) {
        // Do nothing, leaving any previously-created bootstrap CR in place
        log.infof("Bootstrap ConfigMap{namespace=%s, name=%s}: deleted", bootstrap.getMetadata().getNamespace(), bootstrap.getMetadata().getName());

        try {
            client.resource(toModel(bootstrap))
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

    /**
     * De-serialize the model from the bootstrap ConfigMap, assign owners, and
     * create/replace the model CR.
     *
     * Take (non-exclusive) ownership of the bootstrap ConfigMap's namespace to
     * prevent deletion via garbage collection. Additionally, if the namespace is
     * owned by an Addon CR, also make the CR owner of the OperatorObjectModel.
     *
     * <p>
     * This means that:
     *
     * <ol>
     *
     * <li> If this operator is installed as an Addon, deleting the Addon will
     * terminate the OperatorObjectModel CR but keep the operator's namespace. This
     * allows cleanup/finalization to complete.
     *
     * <li> If this operator is installed as a normal deployment or using OLM
     * resources directly, the OperatorObjectModel created from the bootstrap should
     * be deleted _before_ the namespace is removed, also to allow
     * cleanup/finalization to complete.
     *
     * </ol>
     */
    void createOrReplaceModel(ConfigMap bootstrap) {
        var model = toModel(bootstrap);
        var existing = getExisting(model);
        var namespace = client.resources(Namespace.class).withName(client.getNamespace()).get();
        var addonOwner = getAddonOwner(namespace);

        existing.ifPresent(e -> {
            if (e.getOwnerReferenceFor(namespace).isEmpty()) {
                log.infof("Namespace %s will be owned by OperatorObjectModel %s", namespace.getMetadata().getName(), e.getMetadata().getName());
            }

            addonOwner.ifPresent(owner -> {
                if (e.getOwnerReferenceFor(owner.getUid()).isEmpty()) {
                    log.infof("Adding Addon %s as owner of OperatorObjectModel %s", owner.getName(), e.getMetadata().getName());
                }
            });
        });

        addonOwner.map(OwnerReferenceBuilder::new)
                .map(builder -> builder.withBlockOwnerDeletion(Boolean.FALSE))
                .map(builder -> builder.withController(Boolean.FALSE))
                .map(OwnerReferenceBuilder::build)
                .ifPresent(model::addOwnerReference);

        model = client.resource(model).createOrReplace();
        model.own(namespace);

        client.resource(namespace).patch();
    }

    Optional<OwnerReference> getAddonOwner(HasMetadata resource) {
        return Optional.ofNullable(resource.getMetadata().getOwnerReferences())
            .map(Collection::stream)
            .orElseGet(Stream::empty)
            .filter(owner -> "Addon".equals(owner.getKind()))
            .filter(owner -> owner.getApiVersion().startsWith("addons.managed.openshift.io/"))
            .findFirst();
    }

    Optional<OperatorObjectModel> getExisting(OperatorObjectModel model) {
        return primaryCache.list(e -> Objects.equals(e.getMetadata().getName(), model.getMetadata().getName()))
                .findFirst();
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
