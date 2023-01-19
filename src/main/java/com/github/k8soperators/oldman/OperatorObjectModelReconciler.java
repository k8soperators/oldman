package com.github.k8soperators.oldman;

import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModelStatus;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorSource;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedConfigMap;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedData;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedSecret;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroup;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSource;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpecBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

@ControllerConfiguration
public class OperatorObjectModelReconciler implements Reconciler<OperatorObjectModel>, Cleaner<OperatorObjectModel>, EventSourceInitializer<OperatorObjectModel> {

    private final KubernetesClient client;

    @Inject
    Logger log;

    @Inject
    ConfigMapEventSource configMapEventSource;

    @Inject
    SecretEventSource secretEventSource;

    public OperatorObjectModelReconciler(KubernetesClient client) {
        this.client = client;
    }

    @PostConstruct
    void initialize() {
        ConfigMap bootstrap = client.resources(ConfigMap.class).inNamespace(client.getNamespace()).withName("oldman-bootstrap").get();

        if (bootstrap != null) {
            log.infof("Found bootstrap ConfigMap", bootstrap);
            OperatorObjectModel model = Serialization.unmarshal(bootstrap.getData().get("model"), OperatorObjectModel.class);
            Optional.ofNullable(model.getMetadata().getLabels())
                .ifPresentOrElse(
                        labels -> labels.put("app.kubernetes.io/managed-by", "oldman"),
                        () -> model.getMetadata().setLabels(Map.of("app.kubernetes.io/managed-by", "oldman")));
            client.resources(OperatorObjectModel.class).createOrReplace(model);
        } else {
            log.infof("No bootstrap ConfigMap found.");
        }
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<OperatorObjectModel> context) {
        configMapEventSource.setPrimaryCache(context.getPrimaryCache());
        SharedIndexInformer<ConfigMap> configMapInformer = client.configMaps().inNamespace(client.getNamespace()).inform();
        configMapInformer.addEventHandler(configMapEventSource);

        secretEventSource.setPrimaryCache(context.getPrimaryCache());
        SharedIndexInformer<Secret> secretInformer = client.secrets().inNamespace(client.getNamespace()).inform();
        secretInformer.addEventHandler(secretEventSource);

        return Map.of(
                "configMapSource", configMapEventSource,
                "secretSource", secretEventSource);
    }

    @Override
    public UpdateControl<OperatorObjectModel> reconcile(OperatorObjectModel model, Context<OperatorObjectModel> context) {
        /*
         * To-Do List:
         *
         * - Validate the model
         * - Informers for sub-resources
         * - Delete flag for each sub-resource specified in model
         * - Allow labels/annotations on namespaces, configmaps, secrets, and OLM resources in model
         * - Calculate `Installing` conditions based on their status
         * - Calculate `Ready` condition based on presence of both `Error` or `Installing` conditions
         */
        OperatorObjectModelStatus status = model.getOrCreateStatus();
        status.getConditions().removeIf(c -> isCondition(c, "Error", "MissingResource"));

        model.getSpec().getOperators().forEach(operator -> reconcile(model, operator));

        status.getConditions().stream().filter(c -> "Error".equals(c.getType())).findFirst().ifPresentOrElse(
                c -> {
                    Condition readyCondition = status.getOrCreateCondition("Ready");
                    readyCondition.setStatus("False");
                    readyCondition.setReason("ErrorConditions");
                    readyCondition.setMessage(null);
                    readyCondition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
                },
                () -> {
                    Condition readyCondition = status.getOrCreateCondition("Ready");
                    readyCondition.setStatus("True");
                    readyCondition.setReason(null);
                    readyCondition.setMessage(null);
                    readyCondition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
                });

        return UpdateControl.patchStatus(model);
    }

    @Override
    public DeleteControl cleanup(OperatorObjectModel model, Context<OperatorObjectModel> context) {
        model.getSpec()
            .getOperators()
            .stream()
            .map(OperatorSource::getRemoveObjects)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .forEach(obj -> {
                log.infof("Attempting removal of dependent resource: %s", obj);
                client.genericKubernetesResources(obj.getApiVersion(), obj.getKind())
                    .inNamespace(obj.getNamespace())
                    .withName(obj.getName())
                    .delete();
            });

        return DeleteControl.defaultDelete();
    }

    boolean isCondition(Condition condition, String type, String reason) {
        return type.equals(condition.getType()) && reason.equals(condition.getReason());
    }

    void reconcile(OperatorObjectModel model, OperatorSource operator) {
        reconcileNamespace(model, operator);
        reconcileConfigMaps(model, operator);
        reconcileSecrets(model, operator);
        reconcileOperatorLifecycleResources(model, operator);
    }

    void reconcileNamespace(OperatorObjectModel model, OperatorSource operator) {
        String operatorNamespace = operator.getNamespace();
        Resource<Namespace> resource = client.resources(Namespace.class).withName(operatorNamespace);
        Namespace namespace = Optional.ofNullable(resource.get())
                .map(NamespaceBuilder::new)
                .orElseGet(NamespaceBuilder::new)
                .editOrNewMetadata()
                    .withName(operatorNamespace)
                .endMetadata()
                .build();

        addOwnerReference(model, namespace);
        client.namespaces().createOrReplace(namespace);
    }

    static void addOwnerReference(HasMetadata owner, HasMetadata resource) {
        resource.getMetadata().getOwnerReferences().stream()
            .filter(existing ->
                Objects.equals(existing.getName(), owner.getMetadata().getName()) &&
                Objects.equals(existing.getUid(), owner.getMetadata().getUid()))
            .findFirst()
            .ifPresentOrElse(
                    or -> {
                        or.setApiVersion(owner.getApiVersion());
                        or.setKind(owner.getKind());
                        or.setName(owner.getMetadata().getName());
                        or.setUid(owner.getMetadata().getUid());
                    },
                    () ->
                        resource.getMetadata().getOwnerReferences().add(new OwnerReferenceBuilder()
                            .withApiVersion(owner.getApiVersion())
                            .withKind(owner.getKind())
                            .withName(owner.getMetadata().getName())
                            .withUid(owner.getMetadata().getUid())
                            .build()));
    }

    void reconcileConfigMaps(OperatorObjectModel model, OperatorSource operator) {
        Optional.ofNullable(operator.getConfigMaps()).orElseGet(Collections::emptyList).forEach(configuration -> {
            Condition condition = reconcile(configuration, ConfigMap.class, operator.getNamespace(), (source, target) ->
                (target == null ? new ConfigMapBuilder() : new ConfigMapBuilder(target))
                        .editOrNewMetadata()
                            .withNamespace(operator.getNamespace())
                            .withName(configuration.getName())
                        .endMetadata()
                        .withData(source.getData())
                        .withBinaryData(source.getBinaryData())
                        .build());

            if (condition != null) {
                model.getStatus().getConditions().add(condition);
            }
        });
    }

    void reconcileSecrets(OperatorObjectModel model, OperatorSource operator) {
        Optional.ofNullable(operator.getSecrets()).orElseGet(Collections::emptyList).forEach(configuration -> {
            Condition condition = reconcile(configuration, Secret.class, operator.getNamespace(), (source, target) ->
                (target == null ? new SecretBuilder() : new SecretBuilder(target))
                        .editOrNewMetadata()
                            .withNamespace(operator.getNamespace())
                            .withName(configuration.getName())
                        .endMetadata()
                        .withType(source.getType())
                        .withData(source.getData())
                        .withStringData(source.getStringData())
                        .build());

            if (condition != null) {
                model.getStatus().getConditions().add(condition);
            }
        });
    }

    <T extends HasMetadata> Condition reconcile(PropagatedData configuration,
            Class<T> resourceType,
            String targetNamespace,
            BinaryOperator<T> updater) {

        String sourceName = configuration.getSourceName();
        boolean sourceRequired = Boolean.FALSE.equals(configuration.isSourceOptional());

        T source =  client.resources(resourceType)
                .inNamespace(client.getNamespace())
                .withName(sourceName)
                .get();

        if (source == null) {
            if (sourceRequired) {
                return new ConditionBuilder()
                        .withType("Error")
                        .withStatus("True")
                        .withReason("MissingResource")
                        .withMessage(String.format("%s{name=%s} is required", resourceType.getSimpleName(), sourceName))
                        .withLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString())
                        .build();
            }

            return null;
        }

        Resource<T> targetResource = client.resources(resourceType)
                .inNamespace(targetNamespace)
                .withName(configuration.getName());

        T target = targetResource.get();
        boolean resourceExists = (target != null);
        target = updater.apply(source, target);

        if (resourceExists) {
            targetResource.replace(target);
        } else {
            targetResource.create(target);
        }

        return null;
    }

    void reconcileOperatorLifecycleResources(OperatorObjectModel model, OperatorSource operator) {
        final String targetNamespace = operator.getNamespace();

        if (operator.getCatalogSourceSpec() != null) {
            reconcile(operator, CatalogSource.class, "-catalog", (targetName, target) ->
                (target == null ? new CatalogSourceBuilder() : new CatalogSourceBuilder(target))
                        .editOrNewMetadata()
                            .withNamespace(targetNamespace)
                            .withName(targetName)
                        .endMetadata()
                        .withSpec(operator.getCatalogSourceSpec())
                        .build());
        }

        OperatorGroupSpec operatorGroup =
                Optional.ofNullable(operator.getOperatorGroupSpec())
                        .orElseGet(OperatorGroupSpec::new);

        reconcileSingleton(operator, OperatorGroup.class, "-group", (targetName, target) ->
            (target == null ? new OperatorGroupBuilder() : new OperatorGroupBuilder(target))
                    .editOrNewMetadata()
                        .withNamespace(targetNamespace)
                        .withName(targetName)
                    .endMetadata()
                    .withSpec(operatorGroup)
                    .build());

        if (operator.getSubscriptionSpec() != null) {
            reconcile(operator, Subscription.class, "-subscription", (targetName, target) -> {
                var subscription = new SubscriptionSpecBuilder(operator.getSubscriptionSpec());

                if (Boolean.FALSE.equals(subscription.hasSource())) {
                    subscription.withSource(operator.getName() + "-catalog");
                }

                if (Boolean.FALSE.equals(subscription.hasSourceNamespace())) {
                    subscription.withSourceNamespace(targetNamespace);
                }

                return (target == null ? new SubscriptionBuilder() : new SubscriptionBuilder(target))
                        .editOrNewMetadata()
                            .withNamespace(targetNamespace)
                            .withName(targetName)
                        .endMetadata()
                        .withSpec(subscription.build())
                        .build();
            });
        }
    }

    <T extends HasMetadata> void reconcileSingleton(OperatorSource operator,
            Class<T> resourceType,
            String nameSuffix,
            BiFunction<String, T, T> updater) {

        String targetNamespace = operator.getNamespace();
        String targetName = targetNamespace + nameSuffix;

        Resource<T> targetResource = client.resources(resourceType)
                .inNamespace(targetNamespace)
                .withName(targetName);

        T target = targetResource.get();
        boolean resourceExists = (target != null);
        target = updater.apply(targetName, target);

        if (resourceExists) {
            targetResource.replace(target);
        } else {
            targetResource.create(target);
        }
    }

    <T extends HasMetadata> void reconcile(OperatorSource operator,
            Class<T> resourceType,
            String nameSuffix,
            BiFunction<String, T, T> updater) {

        String targetNamespace = operator.getNamespace();
        String operatorName = operator.getName();
        String targetName = operatorName + nameSuffix;

        Resource<T> targetResource = client.resources(resourceType)
                .inNamespace(targetNamespace)
                .withName(targetName);

        T target = targetResource.get();
        boolean resourceExists = (target != null);
        target = updater.apply(targetName, target);

        if (resourceExists) {
            targetResource.replace(target);
        } else {
            targetResource.create(target);
        }
    }

    abstract static class ConfigurationEventSource<T extends HasMetadata> extends AbstractEventSource implements ResourceEventHandler<T> {
        IndexerResourceCache<OperatorObjectModel> primaryCache;

        public void setPrimaryCache(IndexerResourceCache<OperatorObjectModel> primaryCache) {
            this.primaryCache = primaryCache;
        }

        @Override
        public void onAdd(T obj) {
            getReferencingObjectModels(obj).forEach(this::handleEvent);
        }

        @Override
        public void onUpdate(T oldObj, T newObj) {
            getReferencingObjectModels(newObj).forEach(this::handleEvent);
        }

        @Override
        public void onDelete(T obj, boolean deletedFinalStateUnknown) {
            getReferencingObjectModels(obj).forEach(this::handleEvent);
        }

        Stream<ResourceID> getReferencingObjectModels(T obj) {
            return primaryCache.list().filter(model -> isReferenced(model, obj)).map(ResourceID::fromResource);
        }

        void handleEvent(ResourceID modelId) {
            getEventHandler().handleEvent(new ResourceEvent(ResourceAction.UPDATED, modelId, null));
        }

        abstract boolean isReferenced(OperatorObjectModel model, T obj);
    }

    @ApplicationScoped
    public static class ConfigMapEventSource extends ConfigurationEventSource<ConfigMap> {
        @Override
        boolean isReferenced(OperatorObjectModel model, ConfigMap configMap) {
            return model.getSpec()
                    .getOperators()
                    .stream()
                    .map(OperatorSource::getConfigMaps)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .map(PropagatedConfigMap::getSource)
                    .map(ConfigMapKeySelector::getName)
                    .map(name -> configMap.getMetadata().getName().equals(name))
                    .findFirst()
                    .orElse(false);
        }
    }

    @ApplicationScoped
    public static class SecretEventSource extends ConfigurationEventSource<Secret> {
        @Override
        boolean isReferenced(OperatorObjectModel model, Secret secret) {
            return model.getSpec()
                    .getOperators()
                    .stream()
                    .map(OperatorSource::getSecrets)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .map(PropagatedSecret::getSource)
                    .map(SecretKeySelector::getName)
                    .map(name -> secret.getMetadata().getName().equals(name))
                    .findFirst()
                    .orElse(false);
        }
    }
}
