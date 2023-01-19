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
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
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

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.WATCH_CURRENT_NAMESPACE;

@ControllerConfiguration(namespaces = WATCH_CURRENT_NAMESPACE)
public class OperatorObjectModelReconciler implements Reconciler<OperatorObjectModel>, Cleaner<OperatorObjectModel>, EventSourceInitializer<OperatorObjectModel> {

    private final KubernetesClient client;

    @Inject
    ConfigMapEventSource configMapEventSource;

    @Inject
    SecretEventSource secretEventSource;

    public OperatorObjectModelReconciler(KubernetesClient client) {
        this.client = client;
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
        //TODO: validate the model
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
        /*
         * - Delete created resources if no other model is a co-owner.
         */
        return DeleteControl.defaultDelete();
    }

    boolean isCondition(Condition condition, String type, String reason) {
        return type.equals(condition.getType()) && reason.equals(condition.getReason());
    }

    void reconcile(OperatorObjectModel model, OperatorSource operator) {
        OperatorObjectModelStatus status = model.getStatus();
        String targetNamespace = operator.getNamespace();

        client.namespaces().createOrReplace(new NamespaceBuilder()
                .withNewMetadata()
                    .withName(targetNamespace)
                .endMetadata()
                .build());

        Optional.ofNullable(operator.getConfigMaps()).orElseGet(Collections::emptyList).forEach(configuration -> {
            Condition condition = reconcile(configuration, ConfigMap.class, targetNamespace, (source, target) ->
                (target == null ? new ConfigMapBuilder() : new ConfigMapBuilder(target))
                        .editOrNewMetadata()
                            .withNamespace(targetNamespace)
                            .withName(configuration.getName())
                        .endMetadata()
                        .withData(source.getData())
                        .withBinaryData(source.getBinaryData())
                        .build());

            if (condition != null) {
                status.getConditions().add(condition);
            }
        });

        Optional.ofNullable(operator.getSecrets()).orElseGet(Collections::emptyList).forEach(configuration -> {
            Condition condition = reconcile(configuration, Secret.class, targetNamespace, (source, target) ->
                (target == null ? new SecretBuilder() : new SecretBuilder(target))
                        .editOrNewMetadata()
                            .withNamespace(targetNamespace)
                            .withName(configuration.getName())
                        .endMetadata()
                        .withData(source.getData())
                        .withStringData(source.getStringData())
                        .build());

            if (condition != null) {
                status.getConditions().add(condition);
            }
        });

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

        reconcile(operator, OperatorGroup.class, "-group", (targetName, target) ->
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
                    .flatMap(o -> o.getConfigMaps().stream())
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
                    .flatMap(o -> o.getSecrets().stream())
                    .map(PropagatedSecret::getSource)
                    .map(SecretKeySelector::getName)
                    .map(name -> secret.getMetadata().getName().equals(name))
                    .findFirst()
                    .orElse(false);
        }
    }
}
