package com.github.k8soperators.oldman;

import com.github.k8soperators.oldman.api.v1alpha.OperatorObjectModel;
import com.github.k8soperators.oldman.api.v1alpha.OperatorObjectModelStatus;
import com.github.k8soperators.oldman.api.v1alpha.PropagatedConfigMap;
import com.github.k8soperators.oldman.api.v1alpha.PropagatedSecret;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
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
import java.util.Map;
import java.util.stream.Stream;

public class OperatorObjectModelReconciler implements Reconciler<OperatorObjectModel>, EventSourceInitializer<OperatorObjectModel> {

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
        SharedIndexInformer<ConfigMap> configMapInformer = client.configMaps().inNamespace("default").inform();
        configMapInformer.addEventHandler(configMapEventSource);

        secretEventSource.setPrimaryCache(context.getPrimaryCache());
        SharedIndexInformer<Secret> secretInformer = client.secrets().inNamespace("default").inform();
        secretInformer.addEventHandler(secretEventSource);

        return Map.of(
                "configMapSource", configMapEventSource,
                "secretSource", secretEventSource);
    }

    @Override
    public UpdateControl<OperatorObjectModel> reconcile(OperatorObjectModel model, Context<OperatorObjectModel> context) {
        //TODO: validate the model
        OperatorObjectModelStatus status = model.getOrCreateStatus();

        model.getSpec().getConfigMaps().forEach(configMap -> {
            String sourceName = configMap.getSelector().getName();
            ConfigMap source = client.configMaps().withName(sourceName).get();

            if (source == null) {
                if (Boolean.FALSE.equals(configMap.getSelector().getOptional())) {
                    Condition condition = status.getOrCreateCondition("Ready");
                    condition.setStatus("False");
                    condition.setReason(String.format("Missing required ConfigMap{name=%s}", sourceName));
                    condition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
                }

                return;
            }

            client.namespaces().createOrReplace(new NamespaceBuilder()
                    .withNewMetadata()
                        .withName(configMap.getNamespace())
                    .endMetadata()
                    .build());

            client.configMaps()
                .inNamespace(configMap.getNamespace())
                .createOrReplace(new ConfigMapBuilder()
                    .withNewMetadata()
                        .withName(configMap.getName())
                    .endMetadata()
                    .withData(source.getData())
                    .withBinaryData(source.getBinaryData())
                    .build());
        });

        model.getSpec().getSecrets().forEach(secret -> {
            String sourceName = secret.getSelector().getName();
            Secret source = client.secrets().withName(sourceName).get();

            if (source == null) {
                if (Boolean.FALSE.equals(secret.getSelector().getOptional())) {
                    Condition condition = status.getOrCreateCondition("Ready");
                    condition.setStatus("False");
                    condition.setReason(String.format("Missing required Secret{name=%s}", sourceName));
                    condition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
                }

                return;
            }

            client.namespaces().createOrReplace(new NamespaceBuilder()
                    .withNewMetadata()
                        .withName(secret.getNamespace())
                    .endMetadata()
                    .build());

            client.secrets()
                .inNamespace(secret.getNamespace())
                .createOrReplace(new SecretBuilder()
                    .withNewMetadata()
                        .withName(secret.getName())
                    .endMetadata()
                    .withData(source.getData())
                    .withStringData(source.getStringData())
                    .build());
        });

        return UpdateControl.patchStatus(model);
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
                    .getConfigMaps()
                    .stream()
                    .map(PropagatedConfigMap::getSelector)
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
                    .getSecrets()
                    .stream()
                    .map(PropagatedSecret::getSelector)
                    .map(SecretKeySelector::getName)
                    .map(name -> secret.getMetadata().getName().equals(name))
                    .findFirst()
                    .orElse(false);
        }
    }
}
