package com.github.k8soperators.oldman;

import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class LeaderConfiguration extends LeaderElectionConfiguration {
    public LeaderConfiguration() {
        super("oldman-lease");
    }
}
