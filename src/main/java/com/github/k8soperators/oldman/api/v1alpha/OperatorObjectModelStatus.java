package com.github.k8soperators.oldman.api.v1alpha;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OperatorObjectModelStatus {

    public static final String CONDITION_READY = "Ready";

    @JsonProperty("lastUpdated")
    String lastUpdated;

    List<SubscriptionHealth> subscriptionHealth;

    List<Condition> conditions = new ArrayList<>();

    public Condition getOrCreateCondition(String type) {
        return conditions.stream()
                .filter(condition -> Objects.equals(type, condition.getType())).findFirst()
                .orElseGet(() -> {
                    Condition condition = new ConditionBuilder().withType(type).build();
                    conditions.add(condition);
                    return condition;
                });
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
