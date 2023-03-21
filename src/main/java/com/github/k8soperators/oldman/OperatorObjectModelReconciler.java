package com.github.k8soperators.oldman;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorObjectModelStatus;
import com.github.k8soperators.oldman.api.v1alpha1.OperatorSource;
import com.github.k8soperators.oldman.api.v1alpha1.PropagatedData;
import com.github.k8soperators.oldman.api.v1alpha1.Subresource;
import com.github.k8soperators.oldman.events.BootstrapConfigMapEventHandler;
import com.github.k8soperators.oldman.events.ClusterServiceVersionEventSource;
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
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroup;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpec;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupSpecBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupStatus;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSource;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.CatalogSourceStatus;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.ClusterServiceVersionStatus;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpec;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpecBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionStatus;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.inject.Inject;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ControllerConfiguration
public class OperatorObjectModelReconciler implements Reconciler<OperatorObjectModel>, Cleaner<OperatorObjectModel>, EventSourceInitializer<OperatorObjectModel> {

    public static final String LABEL_KEY_MANAGED_BY = "app.kubernetes.io/managed-by";
    public static final String OLDMAN = "oldman";

    private static final String CONDITION_READY = "Ready";
    private static final String CONDITION_INPROGRESS = "InProgress";
    private static final String CONDITION_ERROR = "Error";

    private static final List<String> CONDITIONS_ORDER = List.of(CONDITION_READY, CONDITION_INPROGRESS, CONDITION_ERROR);

    private static final String STATUS_TRUE = "True";
    private static final String STATUS_FALSE = "False";

    static <T> Consumer<T> doNothing() {
        return x -> { };
    }

    private final KubernetesClient client;

    @Inject
    Logger log;

    @Inject
    @ConfigProperty(name = "oldman.bootstrap-configmap-name", defaultValue = "oldman-bootstrap")
    String bootstrapConfigMapName;

    OwnedResourceEventSource ownedResources;
    ClusterServiceVersionEventSource csvEventSource;

    public OperatorObjectModelReconciler(KubernetesClient client) {
        this.client = client;
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<OperatorObjectModel> context) {
        BootstrapConfigMapEventHandler bootstrapHandler = new BootstrapConfigMapEventHandler(client, context.getPrimaryCache());
        SharedIndexInformer<ConfigMap> bootstrapInformer = client.resources(ConfigMap.class).inNamespace(client.getNamespace()).withName(bootstrapConfigMapName).inform();
        bootstrapInformer.addEventHandler(bootstrapHandler);

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

        csvEventSource = new ClusterServiceVersionEventSource(context.getPrimaryCache(), ownedResources, client);

        return Map.of(
                "configMapSource", configMapEventSource,
                "secretSource", secretEventSource,
                "ownedResources", ownedResources,
                "csvEventSource", csvEventSource);
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
        status.getConditions().removeIf(c -> isConditionType(c, CONDITION_ERROR, CONDITION_INPROGRESS));

        model.getSpec().getOperators().forEach(operator -> reconcile(model, operator));

        status.getConditions()
            .stream()
            .filter(c -> isConditionType(c, CONDITION_ERROR, CONDITION_INPROGRESS))
            .filter(c -> "True".equals(c.getStatus()))
            .sorted((c1, c2) -> c1.getType().compareTo(c2.getType())) // Error before InProgress
            .findFirst()
            .ifPresentOrElse(
                    c -> status.updateCondition(CONDITION_READY, STATUS_FALSE, c.getType(), null),
                    () -> status.updateCondition(CONDITION_READY, STATUS_TRUE, "Succeeded", "Operators are installed with no errors detected"));

        initializeConditionIfMissing(status, CONDITION_ERROR);
        initializeConditionIfMissing(status, CONDITION_INPROGRESS);

        // Make the order of conditions stable
        var sortedConditions = status.getConditions()
            .stream()
            .sorted((c1, c2) -> Integer.compare(CONDITIONS_ORDER.indexOf(c1.getType()), CONDITIONS_ORDER.indexOf(c2.getType())))
            .collect(Collectors.toCollection(ArrayList::new));

        status.setConditions(sortedConditions);

        return UpdateControl.patchStatus(model);
    }

    void initializeConditionIfMissing(OperatorObjectModelStatus status, String conditionType) {
        status.getConditions()
            .stream()
            .filter(c -> isConditionType(c, conditionType))
            .findFirst()
            .ifPresentOrElse(doNothing(), () -> status.updateCondition(conditionType, STATUS_FALSE, null, null));
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

        model.own(namespace);

        if (model.isControllingOwner(namespace)) {
            client.resource(namespace).createOrReplace();
        } else {
            addOwnerReference(model, client.resource(namespace).fromServer().get());
        }
    }

    void reconcileConfigMaps(OperatorObjectModel model, OperatorSource operator) {
        Optional.ofNullable(operator.getConfigMaps())
            .map(Collection::stream)
            .orElseGet(Stream::empty)
            .map(configuration -> reconcile(model, operator, configuration, Objects::equals,
                    (source, target) ->
                            Optional.ofNullable(target)
                                .map(ConfigMapBuilder::new)
                                .orElseGet(ConfigMapBuilder::new)
                                .editOrNewMetadata()
                                    .withNamespace(operator.getNamespace())
                                    .withName(configuration.getName())
                                    .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                                .endMetadata()
                                .withData(reconcileDataMap(configuration, source, target, ConfigMap::getData, ConfigMap::getBinaryData))
                                .withBinaryData(reconcileDataMap(configuration, source, target, ConfigMap::getBinaryData, ConfigMap::getData))
                                .build()))
            .filter(Objects::nonNull)
            .forEach(model.getStatus()::mergeCondition);
    }

    void reconcileSecrets(OperatorObjectModel model, OperatorSource operator) {
        Optional.ofNullable(operator.getSecrets())
            .map(Collection::stream)
            .orElseGet(Stream::empty)
            .map(configuration -> reconcile(model, operator, configuration, Objects::equals,
                    (source, target) ->
                            Optional.ofNullable(target)
                                    .map(SecretBuilder::new)
                                    .orElseGet(SecretBuilder::new)
                                    .editOrNewMetadata()
                                        .withNamespace(operator.getNamespace())
                                        .withName(configuration.getName())
                                        .addToLabels(LABEL_KEY_MANAGED_BY, OLDMAN)
                                    .endMetadata()
                                    .withType(Optional.ofNullable(configuration.getType()).orElseGet(source::getType))
                                    .withData(reconcileDataMap(configuration, source, target, Secret::getData, Secret::getStringData))
                                    .withStringData(reconcileDataMap(configuration, source, target, Secret::getStringData, Secret::getData))
                                    .build()))
            .filter(Objects::nonNull)
            .forEach(model.getStatus()::mergeCondition);
    }

    static <T> Map<String, String> reconcileDataMap(PropagatedData<T> configuration,
            T source,
            T target,
            Function<T, Map<String, String>> accessor,
            Function<T, Map<String, String>> alternateAccessor) {

        final Map<String, String> sourceData = Optional.ofNullable(source).map(accessor).orElseGet(Collections::emptyMap);
        final Map<String, String> altSourceData = Optional.ofNullable(source).map(alternateAccessor).orElseGet(Collections::emptyMap);
        final Map<String, String> data = Optional.ofNullable(target)
                .map(accessor)
                .map(HashMap::new)
                .orElseGet(HashMap::new);

        final String sourceKey = configuration.getSourceKey();
        final String key = configuration.getKey();

        if (key != null) {
            if (configuration.isRemoved()) {
                data.remove(key);
            } else {
                if (sourceData.containsKey(sourceKey) && !altSourceData.containsKey(sourceKey)) {
                    data.put(key, sourceData.get(sourceKey));
                }
            }
        } else {
            data.putAll(sourceData);
        }

        return data;
    }

    <T extends HasMetadata> Condition reconcile(OperatorObjectModel model,
            OperatorSource operator,
            PropagatedData<T> configuration,
            BiPredicate<T, T> hasDesiredState,
            BinaryOperator<T> updater) {

        Class<T> resourceType = configuration.getGenericType();
        String sourceName = configuration.getSourceName();

        T source = client.resources(resourceType)
                .inNamespace(client.getNamespace())
                .withName(sourceName)
                .get();

        if (!configuration.isRemoved() && configuration.isSourceMissing(source)) {
            return handleMissingDataSource(configuration, resourceType);
        }

        String targetNamespace = operator.getNamespace();
        String name = configuration.getName();
        T current = ownedResources.get(resourceType, targetNamespace, name);

        if (configuration.isRemoved() && configuration.getKey() == null) {
            // Configuration says to remove the entire resource
            if (current != null) {
                var details = client.resource(current).delete();
                log.debugf("%s{namespace=%s, name=%s}: deleted. Details: %s", resourceType.getSimpleName(), targetNamespace, name, details);
            }
            return null;
        }

        T desired = updater.apply(source, current);

        reconcile(model, operator, current, desired, hasDesiredState);
        return null;
    }

    Condition handleMissingDataSource(PropagatedData<?> configuration, Class<?> resourceType) {
        if (!configuration.isSourceOptional()) {
            String sourceName = configuration.getSourceName();
            String message;

            if (configuration.getSourceKey() != null) {
                message = String.format("%s{name=%s, key=%s} is required", resourceType.getSimpleName(), sourceName, configuration.getSourceKey());
            } else {
                message = String.format("%s{name=%s} is required", resourceType.getSimpleName(), sourceName);
            }

            return new ConditionBuilder()
                    .withType(CONDITION_ERROR)
                    .withStatus("True")
                    .withReason("MissingResource")
                    .withMessage(message)
                    .withLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString())
                    .build();
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

        if (operator.getCatalogSource() == null) {
            CatalogSource catalog = ownedResources.get(CatalogSource.class,
                    operator.getNamespace(),
                    Subresource.name(CatalogSource.class, operator));

            if (catalog != null) {
                client.resource(catalog).delete();
                log.infof("Orphaned %s{namespace=%s, name=%s}: deleted", catalog.getKind(), catalog.getMetadata().getNamespace(), catalog.getMetadata().getName());
            }
        }
    }

    void reconcileCatalogSource(OperatorObjectModel model, OperatorSource operator, Subresource<CatalogSource, CatalogSourceSpec> subresource) {
        CatalogSource result = reconcile(model, operator, subresource, (name, existing) ->
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

        Optional.ofNullable(result.getStatus())
            .map(CatalogSourceStatus::getConditions)
            .filter(Predicate.not(Collection::isEmpty))
            .ifPresent(conditions ->
                    model.getStatus().mergeCondition(new ConditionBuilder()
                            .withType(CONDITION_ERROR)
                            .withStatus(STATUS_TRUE)
                            .withMessage(String.format(
                                    "CatalogSource %s/%s has conditions: [ %s ]",
                                    operator.getNamespace(),
                                    subresource.name(operator),
                                    conditions.stream().map(Condition::getMessage).collect(Collectors.joining("; "))))
                            .build()));
    }

    void reconcileOperatorGroup(OperatorObjectModel model, OperatorSource operator, Subresource<OperatorGroup, OperatorGroupSpec> operatorGroup) {
        Subresource<OperatorGroup, OperatorGroupSpec> subresource =
                Optional.ofNullable(operatorGroup).orElseGet(() -> Subresource.newInstance("OperatorGroup"));

        UnaryOperator<OperatorGroupSpecBuilder> ensureUpgradeStrategy = builder -> {
            builder.getAdditionalProperties().computeIfAbsent("upgradeStrategy", key -> "Default");
            return builder;
        };

        OperatorGroup result = reconcile(model, operator, subresource, (name, existing) ->
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
                    .withSpec(
                            ensureUpgradeStrategy.apply(Optional.of(subresource)
                                    .map(Subresource::getSpec)
                                    .map(OperatorGroupSpecBuilder::new)
                                    .orElseGet(OperatorGroupSpecBuilder::new))
                                .build())
                    .build());

        Optional.ofNullable(result.getStatus())
            .map(OperatorGroupStatus::getConditions)
            .filter(Predicate.not(Collection::isEmpty))
            .ifPresent(conditions ->
                    model.getStatus().mergeCondition(new ConditionBuilder()
                            .withType(CONDITION_ERROR)
                            .withStatus(STATUS_TRUE)
                            .withMessage(String.format(
                                    "OperatorGroup %s/%s has condition(s): [ %s ]",
                                    operator.getNamespace(),
                                    subresource.name(operator),
                                    conditions.stream().map(Condition::getMessage).collect(Collectors.joining("; "))))
                            .build()));
    }

    void reconcileSubscription(OperatorObjectModel model, OperatorSource operator, Subresource<Subscription, SubscriptionSpec> subresource) {
        Subscription result = reconcile(model, operator, subresource, (name, existing) -> {
            var subscriptionSpec = new SubscriptionSpecBuilder(subresource.getSpec());

            if (Boolean.FALSE.equals(subscriptionSpec.hasSource())) {
                subscriptionSpec.withSource(Subresource.name(CatalogSource.class, operator));
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

        Optional.ofNullable(result.getStatus())
                .map(SubscriptionStatus::getConditions)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .filter(Predicate.not(condition -> "CatalogSourcesUnhealthy".equals(condition.getType()) && STATUS_FALSE.equals(condition.getStatus())))
                .forEach(condition -> model.getStatus().mergeCondition(new ConditionBuilder()
                        .withType("InstallPlanPending".equals(condition.getType()) ? CONDITION_INPROGRESS : CONDITION_ERROR)
                        .withStatus(STATUS_TRUE)
                        .withMessage(String.format(
                                "Subscription %s/%s has condition: {type=%s, status=%s, reason=%s, message=%s}",
                                operator.getNamespace(),
                                subresource.name(operator),
                                condition.getType(),
                                condition.getStatus(),
                                condition.getReason(),
                                condition.getMessage()))
                        .build()));

        Optional.ofNullable(result.getStatus())
            .map(SubscriptionStatus::getInstalledCSV)
            .map(installedCsv -> csvEventSource.get(operator.getNamespace(), installedCsv))
            .filter(installedCsv -> Optional.ofNullable(installedCsv.getStatus()).map(ClusterServiceVersionStatus::getPhase).isPresent())
            .ifPresentOrElse(
                    installedCsv -> {
                        var csvStatus = installedCsv.getStatus();

                        switch (csvStatus.getPhase()) {
                        case "Succeeded":
                            break;

                        case "Pending":
                        case "InstallReady":
                        case "Installing":
                            model.getStatus().mergeCondition(new ConditionBuilder()
                                    .withType(CONDITION_INPROGRESS)
                                    .withStatus(STATUS_TRUE)
                                    .withMessage(String.format(
                                            "ClusterServiceVersion %s/%s not yet ready: {phase=%s, reason=%s, message=%s}",
                                            installedCsv.getMetadata().getNamespace(),
                                            installedCsv.getMetadata().getName(),
                                            csvStatus.getPhase(),
                                            csvStatus.getReason(),
                                            csvStatus.getMessage()))
                                    .build());
                            break;

                        default:
                            model.getStatus().mergeCondition(new ConditionBuilder()
                                    .withType(CONDITION_ERROR)
                                    .withStatus(STATUS_TRUE)
                                    .withMessage(String.format(
                                            "ClusterServiceVersion %s/%s has invalid status: {phase=%s, reason=%s, message=%s}",
                                            installedCsv.getMetadata().getNamespace(),
                                            installedCsv.getMetadata().getName(),
                                            csvStatus.getPhase(),
                                            csvStatus.getReason(),
                                            csvStatus.getMessage()))
                                    .build());
                            break;
                        }
                    },
                    () -> model.getStatus().mergeCondition(new ConditionBuilder()
                                .withType(CONDITION_INPROGRESS)
                                .withStatus(STATUS_TRUE)
                                .withMessage(String.format(
                                        "ClusterServiceVersion for Subscription %s/%s not yet created or not reporting status",
                                        operator.getNamespace(),
                                        subresource.name(operator)))
                                .build()));
    }

    <K extends HasMetadata, S> K reconcile(
            OperatorObjectModel model,
            OperatorSource operator,
            Subresource<K, S> subresource,
            BiFunction<String, K, K> updater) {

        Class<K> resourceType = subresource.getKindType();
        String name = subresource.name(operator);
        String targetNamespace = operator.getNamespace();
        K current = ownedResources.get(resourceType, targetNamespace, name);
        K desired = updater.apply(name, current);

        if (desired.getMetadata().getAnnotations().isEmpty()) {
            desired.getMetadata().setAnnotations(null);
        }

        return reconcile(model, operator, current, desired, subresource::hasDesiredState);
    }

    <K extends HasMetadata> K reconcile(OperatorObjectModel model,
            OperatorSource operator,
            K current,
            K desired,
            BiPredicate<K, K> hasDesiredState) {

        final String namespace = operator.getNamespace();
        final String kind = desired.getMetadata().getNamespace();
        final String name = desired.getMetadata().getName();

        model.own(desired);

        if (current != null) {
            if (hasDesiredState.test(current, desired)) {
                log.tracef("%s{namespace=%s, name=%s}: unchanged", kind, namespace, name);
                return current;
            } else if (model.isControllingOwner(desired)) {
                logChanged(current, desired);
                return client.resource(desired).replace();
            } else {
                log.debugf("%s{namespace=%s, name=%s} changed by %s, but it is not the controlling owner. Resource will not be updated",
                        desired.getKind(),
                        desired.getMetadata().getNamespace(),
                        desired.getMetadata().getName(),
                        model.getMetadata().getName());

                return addOwnerReference(model, current);
            }
        } else {
            log.debugf("%s{namespace=%s, name=%s}: created", kind, namespace, name);
            return client.resource(desired).create();
        }
    }

    <K extends HasMetadata> K addOwnerReference(OperatorObjectModel model, K current) {
        return client.resource(current).edit(existing -> {
            model.own(existing);
            return existing;
        });
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
