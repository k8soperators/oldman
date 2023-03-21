package com.github.k8soperators.oldman.api.v1alpha1;

import org.jboss.logging.Logger;

import java.util.Map;

public interface PropagatedData<T> {

    static final Logger log = Logger.getLogger(PropagatedData.class);

    String getSourceName();

    String getSourceKey();

    default boolean isSourceMissing(T source) {
        if (source == null) {
            return true;
        }

        String sourceKey = getSourceKey();

        if (sourceKey == null) {
            return false;
        }

        return !hasKey(source, sourceKey);
    }

    boolean isSourceOptional();

    String getName();

    String getKey();

    boolean isRemoved();

    Class<T> getGenericType();

    boolean hasKey(T source, String key);

    default boolean hasKey(Map<String, ?> data, String key) {
        return data != null && data.containsKey(key);
    }

}
