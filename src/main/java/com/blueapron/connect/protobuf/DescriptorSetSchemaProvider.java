package com.blueapron.connect.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schema provider that uses a provided FileDescriptorSet to build a registry of known types.
 * Created by alex.reid on 19/03/2017.
 */
public class DescriptorSetSchemaProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DescriptorSetSchemaProvider.class);
    private ConcurrentHashMap<String, Descriptors.Descriptor> lookup;
    private String descriptorPath = null;

    public DescriptorSetSchemaProvider(DescriptorProtos.FileDescriptorSet descriptorSet) throws Exception {
        loadDescriptorSet(descriptorSet);
    }

    public DescriptorSetSchemaProvider(String path) throws Exception {
        this.descriptorPath = path;
        refresh();
    }

    public synchronized void refresh() throws Exception {
        LOG.info("Loading message descriptions from {}", this.descriptorPath);
        FileInputStream input = new FileInputStream(this.descriptorPath);
        DescriptorProtos.FileDescriptorSet descriptor = DescriptorProtos.FileDescriptorSet.parseFrom(input);

        loadDescriptorSet(descriptor);
    }

    private void loadDescriptorSet(DescriptorProtos.FileDescriptorSet descriptorSet) throws Exception {
        if (lookup == null) {
            lookup = new ConcurrentHashMap<>();
        } else {
            lookup.clear();
        }

        Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptors = descriptorSet.getFileList().stream().reduce(
                new HashMap<>(),
                (map, fdp) -> {
                    map.put(fdp.getName(), fdp);
                    return map;
                },
                (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                }
        );

        Map<String, Descriptors.FileDescriptor> fileDescriptorCache = new HashMap<>();

        for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
            Descriptors.FileDescriptor fd = buildDescriptorFromProto(fdp, fileDescriptors, fileDescriptorCache);
            for (Descriptors.Descriptor messageType : fd.getMessageTypes()) {
                lookup.put(messageType.getFullName(), messageType);
                LOG.debug("Discovered: {}", messageType.getFullName());
            }
        }
        LOG.info("Discovered {} message types from descriptor set.", lookup.size());
    }

    private Descriptors.FileDescriptor buildDescriptorFromProto(
            DescriptorProtos.FileDescriptorProto fileDescriptorProto,
            Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap,
            Map<String, Descriptors.FileDescriptor> fileDescriptorCache
    ) throws Descriptors.DescriptorValidationException {
        String descriptorName = fileDescriptorProto.getName();

        if (fileDescriptorCache.containsKey(descriptorName)) {
            return fileDescriptorCache.get(descriptorName);
        }

        int dependencyCount = fileDescriptorProto.getDependencyCount();

        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[fileDescriptorProto.getDependencyCount()];
        for (int i = 0; i < dependencyCount; i++) {
            String dependencyName = fileDescriptorProto.getDependency(i);

            if (!fileDescriptorProtoMap.containsKey(dependencyName)) {
                continue;
            }

            DescriptorProtos.FileDescriptorProto dependency = fileDescriptorProtoMap.get(dependencyName);
            dependencies[i] = buildDescriptorFromProto(dependency, fileDescriptorProtoMap, fileDescriptorCache);
        }

        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies, true);
        fileDescriptorCache.put(fileDescriptor.getName(), fileDescriptor);
        return fileDescriptor;
    }

    public Descriptors.Descriptor getDescriptorForTypeName(String typeName) throws ConnectException {
        if (lookup.get(typeName) != null) {
            return lookup.get(typeName);
        } else {
            throw new ConnectException("Unknown type " + typeName + " found in loaded registry.");
        }
    }
}
