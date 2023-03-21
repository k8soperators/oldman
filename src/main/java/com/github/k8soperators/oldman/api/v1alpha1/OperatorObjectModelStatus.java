package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class OperatorObjectModelStatus extends ObservedGenerationAwareStatus {

    @JsonProperty("lastUpdated")
    String lastUpdated;

    List<SubscriptionHealth> subscriptionHealth;

    List<Condition> conditions = new ArrayList<>();

    @JsonIgnore
    public Condition getOrCreateCondition(String type) {
        return getCondition(type).orElseGet(() -> {
            Condition condition = new ConditionBuilder().withType(type).build();
            conditions.add(condition);
            return condition;
        });
    }

    @JsonIgnore
    private Optional<Condition> getCondition(String type) {
        return conditions.stream()
                .filter(condition -> Objects.equals(type, condition.getType()))
                .findFirst();
    }

    @JsonIgnore
    public void updateCondition(String type, String status, String reason, String message) {
        Condition condition = getOrCreateCondition(type);

        boolean hasTransitioned = !Objects.equals(status, condition.getStatus()) ||
                !Objects.equals(reason, condition.getReason()) ||
                !Objects.equals(message, condition.getMessage());

        condition.setStatus(status);
        condition.setReason(reason);
        condition.setMessage(message);

        if (hasTransitioned) {
            condition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());
        }
    }

    @JsonIgnore
    public void mergeCondition(Condition newCondition) {
        newCondition.setLastTransitionTime(ZonedDateTime.now(ZoneOffset.UTC).toString());

        getCondition(newCondition.getType()).ifPresentOrElse(
                cond -> {
                    if (newCondition.getMessage() == null || newCondition.getMessage().isBlank()) {
                        return;
                    }

                    StringBuilder message = new StringBuilder();

                    if (cond.getMessage() != null && !cond.getMessage().isBlank()) {
                        message.append(cond.getMessage());
                        message.append("; ");
                    }

                    message.append(newCondition.getMessage());
                    cond.setLastTransitionTime(newCondition.getLastTransitionTime());
                },
                () -> conditions.add(newCondition));
    }

    public List<SubscriptionHealth> getSubscriptionHealth() {
        return subscriptionHealth;
    }

    public void setSubscriptionHealth(List<SubscriptionHealth> subscriptionHealth) {
        this.subscriptionHealth = subscriptionHealth;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }
}
