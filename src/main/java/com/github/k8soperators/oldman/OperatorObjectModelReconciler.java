package com.github.k8soperators.oldman;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModelStatus;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorSource;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedData;
import com.github.k8soperators.oldman.api.v1alpha1.Subresource;
import com.github.k8soperators.oldman.events.BootstrapConfigMapEventHandler;
import com.github.k8soperators.oldman.events.ConfigMapEventSource;
import com.github.k8soperators.oldman.events.OwnedResourceEventSource;
import com.github.k8soperators.oldman.events.SecretEventSource;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroup;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSource;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpecBuilder;
import io.fabric8.zjsonpatch.JsonDiff;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

@ControllerConfiguration
public class OperatorObjectModelReconciler implements Reconciler<OperatorObjectModel>, Cleaner<OperatorObjectModel>, EventSourceInitializer<OperatorObjectModel> {

    public static final String LABEL_KEY_MANAGED_BY = "app.kubernetes.io/managed-by";
    public static final String OLDMAN = "oldman";

    private static final String CONDITION_ERROR = "Error";
    private static final String CONDITION_READY = "Ready";

    private final KubernetesClient client;

    @Inject
    Logger log;

    OwnedResourceEventSource ownedResources;
    SharedIndexInformer<ConfigMap> bootstrapInformer;

    public OperatorObjectModelReconciler(KubernetesClient client) {
        this.client = client;
    }

    @PostConstruct
    void initialize() {
        bootstrapInformer = client.resources(ConfigMap.class).inNamespace(client.getNamespace()).withName("oldman-bootstrap").inform();
        bootstrapInformer.addEventHandler(new BootstrapConfigMapEventHandler(client));
    }

    @PreDestroy
    void shutdown() {
        bootstrapInformer.close();
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<OperatorObjectModel> context) {
        ConfigMapEventSource configMapEventSource = new ConfigMapEventSource(context.getPrimaryCache());
        SharedIndexInformer<ConfigMap> configMapInformer = client.configMaps().inNamespace(client.getNamespace()).inform();
        configMapInformer.addEventHandler(configMapEventSource);

        SecretEventSource secretEventSource = new SecretEventSource(context.getPrimaryCache());
        SharedIndexInformer<Secret> secretInformer = client.secrets().inNamespace(client.getNamespace()).inform();
        secretInformer.addEventHandler(secretEventSource);

        ownedResources = new OwnedResourceEventSource(context.getPrimaryCache());
        ownedResources.addInformer(Namespace.class, client);
        ownedResources.addInformer(ConfigMap.class, client);
        ownedResources.addInformer(Secret.class, client);
        ownedResources.addInformer(CatalogSource.class, client);
        ownedResources.addInformer(OperatorGroup.class, client);
        ownedResources.addInformer(Subscription.class, client);

        return Map.of(
                "configMapSource", configMapEventSource,
                "secretSource", secretEventSource,
                "ownedResources", ownedResources);
    }

    @Override
    public UpdateControl<OperatorObjectModel> reconcile(OperatorObjectModel model, Context<OperatorObjectModel> context) {
        /*
         * To-Do List:
         *
         * - Validate the model
         * - Delete flag for each sub-resource specified in model
         * - Allow labels/annotations on namespaces, configmaps, secrets, and OLM resources in model
         * - Calculate `Installing` conditions based on their status
         * - Calculate `Ready` condition based on presence of both `Error` or `Installing` conditions
         */
        OperatorObjectModelStatus status = model.getOrCreateStatus();
        status.getConditions().removeIf(c -> isConditionType(c, CONDITION_ERROR, "Installing"));

        model.getSpec().getOperators().forEach(operator -> reconcile(model, operator));

        status.getConditions().stream().filter(c -> isConditionType(c, CONDITION_ERROR)).findFirst().ifPresentOrElse(
                c -> {
                    Condition readyCondition = status.getOrCreateCondition(CONDITION_READY);
                    readyCondition.setStatus("False");
                    readyCondition.setReason("ErrorConditions");
                    readyCondition.setMessage(null);
                    readyCondition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
                },
                () -> {
                    Condition readyCondition = status.getOrCreateCondition(CONDITION_READY);
                    readyCondition.setStatus("True");
                    readyCondition.setReason(null);
                    readyCondition.setMessage(null);
                    readyCondition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
                });

        return UpdateControl.patchStatus(model);
    }

    @Override
    public DeleteControl cleanup(OperatorObjectModel model, Context<OperatorObjectModel> context) {
        boolean remaining = model.getSpec()
            .getOperators()
            .stream()
            .map(OperatorSource::getCleanupResources)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .map(this::cleanupResource)
            .filter(Boolean.TRUE::equals)
            .distinct() // ensure all entries processed
            .findFirst()
            .orElse(false);

        if (remaining) {
            return DeleteControl.noFinalizerRemoval()
                .rescheduleAfter(Duration.ofSeconds(5));
        }

        return DeleteControl.defaultDelete();
    }

    boolean cleanupResource(ObjectReference obj) {
        var resourceClient = client.genericKubernetesResources(obj.getApiVersion(), obj.getKind())
                .inNamespace(obj.getNamespace())
                .withName(obj.getName());

        var resource = resourceClient.get();

        if (resource != null) {
            if (resource.getMetadata().getDeletionTimestamp() == null) {
                log.infof("Attempting removal of dependent resource: %s", obj);
                resourceClient.delete();
            } else {
                log.debugf("Dependent resource already deleted and pending removal: %s", obj);
            }
            return true;
        }

        return false;
    }

    boolean isCondition(Condition condition, String type, String reason) {
        return type.equals(condition.getType()) && reason.equals(condition.getReason());
    }

    boolean isConditionType(Condition condition, String... types) {
        return Arrays.asList(types).contains(condition.getType());
    }

    void reconcile(OperatorObjectModel model, OperatorSource operator) {
        reconcileNamespace(model, operator);
        reconcileConfigMaps(model, operator);
        reconcileSecrets(model, operator);
        reconcileOperatorLifecycleResources(model, operator);
    }

    void reconcileNamespace(OperatorObjectModel model, OperatorSource operator) {
        String operatorNamespace = operator.getNamespace();

        Namespace namespace = Optional.ofNullable(ownedResources.get(Namespace.class, operatorNamespace))
                .map(NamespaceBuilder::new)
                .orElseGet(NamespaceBuilder::new)
                .editOrNewMetadata()
                    .withName(operatorNamespace)
                    .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                .endMetadata()
                .build();

        addOwnerReference(model, namespace);
        client.resource(namespace).createOrReplace();
    }

    static void addOwnerReference(HasMetadata owner, HasMetadata resource) {
        int ownerCount = resource.optionalMetadata()
                .map(ObjectMeta::getOwnerReferences)
                .map(Collection::size)
                .orElse(0);

        resource.getOwnerReferenceFor(owner)
            .ifPresentOrElse(
                    or -> {
                        or.setApiVersion(owner.getApiVersion());
                        or.setKind(owner.getKind());
                        or.setName(owner.getMetadata().getName());
                        or.setUid(owner.getMetadata().getUid());
                        or.setController(ownerCount == 1 || Boolean.TRUE.equals(or.getController()));
                    },
                    () ->
                        resource.addOwnerReference(new OwnerReferenceBuilder()
                            .withApiVersion(owner.getApiVersion())
                            .withKind(owner.getKind())
                            .withName(owner.getMetadata().getName())
                            .withUid(owner.getMetadata().getUid())
                            .withController(ownerCount == 0)
                            .build()));
    }

    void reconcileConfigMaps(OperatorObjectModel model, OperatorSource operator) {
        Optional.ofNullable(operator.getConfigMaps())
            .map(Collection::stream)
            .orElseGet(Stream::empty)
            .map(configuration ->
                reconcile(model, configuration, ConfigMap.class, operator.getNamespace(), (source, target) ->
                    Optional.ofNullable(target)
                        .map(ConfigMapBuilder::new)
                        .orElseGet(ConfigMapBuilder::new)
                        .editOrNewMetadata()
                            .withNamespace(operator.getNamespace())
                            .withName(configuration.getName())
                            .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                        .endMetadata()
                        .withData(source.getData())
                        .withBinaryData(source.getBinaryData())
                        .build()))
            .filter(Objects::nonNull)
            .forEach(model.getStatus().getConditions()::add);
    }

    void reconcileSecrets(OperatorObjectModel model, OperatorSource operator) {
        Optional.ofNullable(operator.getSecrets())
            .map(Collection::stream)
            .orElseGet(Stream::empty)
            .map(configuration ->
                reconcile(model, configuration, Secret.class, operator.getNamespace(), (source, target) ->
                    Optional.ofNullable(target)
                        .map(SecretBuilder::new)
                        .orElseGet(SecretBuilder::new)
                        .editOrNewMetadata()
                            .withNamespace(operator.getNamespace())
                            .withName(configuration.getName())
                            .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                        .endMetadata()
                        .withType(source.getType())
                        .withData(source.getData())
                        .withStringData(source.getStringData())
                        .build()))
            .filter(Objects::nonNull)
            .forEach(model.getStatus().getConditions()::add);
    }

    <T extends HasMetadata> Condition reconcile(OperatorObjectModel model,
            PropagatedData configuration,
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
                        .withType(CONDITION_ERROR)
                        .withStatus("True")
                        .withReason("MissingResource")
                        .withMessage(String.format("%s{name=%s} is required", resourceType.getSimpleName(), sourceName))
                        .withLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString())
                        .build();
            }

            return null;
        }

        T current = ownedResources.get(resourceType, targetNamespace, configuration.getName());
        T desired = updater.apply(source, current);
        addOwnerReference(model, desired);

        String name = configuration.getName();

        if (current != null) {
            if (Objects.equals(current, desired)) {
                log.tracef("%s{namespace=%s, name=%s}: unchanged", resourceType.getSimpleName(), targetNamespace, name);
            } else {
                logChanged(current, desired);
                client.resource(desired).replace();
            }
        } else {
            log.debugf("%s{namespace=%s, name=%s}: created", resourceType.getSimpleName(), targetNamespace, name);
            client.resource(desired).create();
        }

        return null;
    }

    void reconcileOperatorLifecycleResources(OperatorObjectModel model, OperatorSource operator) {
        if (operator.getCatalogSource() != null) {
            reconcileCatalogSource(model, operator, operator.getCatalogSource());
        }

        reconcileOperatorGroup(model, operator, operator.getOperatorGroup());

        if (operator.getSubscription() != null) {
            reconcileSubscription(model, operator, operator.getSubscription());
        }
    }

    void reconcileCatalogSource(OperatorObjectModel model, OperatorSource operator, Subresource<CatalogSourceSpec> subresource) {
        reconcile(model, operator, CatalogSource.class, operator.getName() + "-catalog",
                (existing, desired) ->
                    Objects.equals(existing.getMetadata(), desired.getMetadata()) &&
                            Objects.equals(existing.getSpec(), desired.getSpec()),
                (name, existing) ->
                    Optional.ofNullable(existing)
                        .map(CatalogSourceBuilder::new)
                        .orElseGet(CatalogSourceBuilder::new)
                        .editOrNewMetadata()
                            .withNamespace(operator.getNamespace())
                            .withName(name)
                            .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                            .addToLabels(subresource.getLabels())
                            .addToAnnotations(subresource.getAnnotations())
                            .removeFromLabels(subresource.getLabelsRemoved())
                            .removeFromAnnotations(subresource.getAnnotationsRemoved())
                        .endMetadata()
                        .withSpec(subresource.getSpec())
                        .build());
    }

    void reconcileOperatorGroup(OperatorObjectModel model, OperatorSource operator, Subresource<OperatorGroupSpec> operatorGroup) {
        Subresource<OperatorGroupSpec> subresource =
                Optional.ofNullable(operatorGroup).orElseGet(Subresource::new);

        reconcile(model, operator, OperatorGroup.class, operator.getNamespace() + "-group",
                (existing, desired) ->
                    Objects.equals(existing.getMetadata(), desired.getMetadata()) &&
                            Objects.equals(existing.getSpec(), desired.getSpec()),
                (name, existing) ->
                    Optional.ofNullable(existing)
                        .map(OperatorGroupBuilder::new)
                        .orElseGet(OperatorGroupBuilder::new)
                        .editOrNewMetadata()
                            .withNamespace(operator.getNamespace())
                            .withName(name)
                            .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                            .addToLabels(subresource.getLabels())
                            .addToAnnotations(subresource.getAnnotations())
                            .removeFromLabels(subresource.getLabelsRemoved())
                            .removeFromAnnotations(subresource.getAnnotationsRemoved())
                        .endMetadata()
                        .withSpec(Optional.of(subresource)
                                .map(Subresource::getSpec)
                                .orElseGet(OperatorGroupSpec::new))
                        .build());
    }

    void reconcileSubscription(OperatorObjectModel model, OperatorSource operator, Subresource<SubscriptionSpec> subresource) {
        reconcile(model, operator, Subscription.class, operator.getName() + "-subscription",
                (existing, desired) ->
                    Objects.equals(existing.getMetadata(), desired.getMetadata()) &&
                            Objects.equals(existing.getSpec(), desired.getSpec()),
                (name, existing) -> {
                    var subscriptionSpec = new SubscriptionSpecBuilder(subresource.getSpec());

                    if (Boolean.FALSE.equals(subscriptionSpec.hasSource())) {
                        subscriptionSpec.withSource(operator.getName() + "-catalog");
                    }

                    if (Boolean.FALSE.equals(subscriptionSpec.hasSourceNamespace())) {
                        subscriptionSpec.withSourceNamespace(operator.getNamespace());
                    }

                    return Optional.ofNullable(existing)
                            .map(SubscriptionBuilder::new)
                            .orElseGet(SubscriptionBuilder::new)
                            .editOrNewMetadata()
                                .withNamespace(operator.getNamespace())
                                .withName(name)
                                .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                                .addToLabels(subresource.getLabels())
                                .addToAnnotations(subresource.getAnnotations())
                                .removeFromLabels(subresource.getLabelsRemoved())
                                .removeFromAnnotations(subresource.getAnnotationsRemoved())
                            .endMetadata()
                            .withSpec(subscriptionSpec.build())
                            .build();
                });
    }

    <T extends HasMetadata> void reconcile(
            OperatorObjectModel model,
            OperatorSource operator,
            Class<T> resourceType,
            String name,
            BiPredicate<T, T> hasDesiredState,
            BiFunction<String, T, T> updater) {

        String targetNamespace = operator.getNamespace();
        T current = ownedResources.get(resourceType, targetNamespace, name);
        T desired = updater.apply(name, current);

        if (desired.getMetadata().getAnnotations().isEmpty()) {
            desired.getMetadata().setAnnotations(null);
        }

        addOwnerReference(model, desired);

        if (current != null) {
            if (hasDesiredState.test(current, desired)) {
                log.tracef("%s{namespace=%s, name=%s}: unchanged", resourceType.getSimpleName(), targetNamespace, name);
            } else {
                logChanged(current, desired);
                client.resource(desired).replace();
            }
        } else {
            log.debugf("%s{namespace=%s, name=%s}: created", resourceType.getSimpleName(), targetNamespace, name);
            client.resource(desired).create();
        }
    }

    void logChanged(HasMetadata current, HasMetadata desired) {
        if (log.isDebugEnabled()) {
            String kind = desired.getKind();
            String namespace = desired.getMetadata().getNamespace();
            String name = desired.getMetadata().getName();
            ObjectMapper objectMapper = Serialization.yamlMapper();
            JsonNode currentJson = objectMapper.convertValue(current, JsonNode.class);
            JsonNode desiredJson = objectMapper.convertValue(desired, JsonNode.class);
            JsonNode patch = JsonDiff.asJson(currentJson, desiredJson);
            log.debugf("%s{namespace=%s, name=%s}: changed =>\n%s", kind, name, namespace, patch.toPrettyString());
        }
    }
}
