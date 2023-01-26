package com.github.k8soperators.oldman.api.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MetadataPairs {

    private Map<String, String> labels = new HashMap<>(0);
    private Map<String, String> annotations = new HashMap<>(0);

    @JsonIgnore
    static Map<String, String> getPairs(Map<String, String> source, Predicate<Map.Entry<String, String>> predicate) {
        return Optional.ofNullable(source)
                .map(Map::entrySet)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .filter(predicate)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @JsonIgnore
    static boolean removed(Map.Entry<String, String> entry) {
        return entry.getKey().startsWith("-");
    }

    @JsonIgnore
    public Map<String, String> getLabelsSet() {
        return getPairs(labels, Predicate.not(MetadataPairs::removed));
    }

    @JsonIgnore
    public Map<String, String> getLabelsRemoved() {
        return getPairs(labels, MetadataPairs::removed);
    }

    @JsonIgnore
    public Map<String, String> getAnnotationsSet() {
        return getPairs(annotations, Predicate.not(MetadataPairs::removed));
    }

    @JsonIgnore
    public Map<String, String> getAnnotationsRemoved() {
        return getPairs(annotations, MetadataPairs::removed);
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
    }

}
