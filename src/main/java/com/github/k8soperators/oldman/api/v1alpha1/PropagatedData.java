package com.github.k8soperators.oldman.api.v1alpha1;

import java.util.Map;

public interface PropagatedData<T> {

    String getSourceName();

    String getSourceKey();

    boolean isSourceMissing(T source);

    boolean isSourceOptional();

    String getName();

    String getKey();

    boolean isRemoved();

    Class<T> getGenericType();

    default boolean hasKey(Map<String, ?> data, String key) {
        return data != null && data.containsKey(key);
    }

}
