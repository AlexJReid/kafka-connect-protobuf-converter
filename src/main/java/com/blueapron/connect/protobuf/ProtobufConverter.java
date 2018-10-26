package com.blueapron.connect.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Implementation of Converter that uses Protobufs.
 */
public class ProtobufConverter implements Converter {
  private static final Logger log = LoggerFactory.getLogger(ProtobufConverter.class);

  private static final String LEGACY_NAME_CONFIG = "legacyName";
  private static final String PROTOBUF_DESCRIPTOR_SET = "protoFileDescriptorSet";
  private static final String PROTOBUF_TYPE = "protoTypeName";
  private static final String PROTOBUF_DESCRIPTOR_SET_DEFAULT = "/opt/connect/messages.fds";

  private ProtobufData protobufData;

  private boolean isInvalidConfiguration(Object proto, boolean isKey) {
    return proto == null && !isKey;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Object legacyName = configs.get(LEGACY_NAME_CONFIG);
    String legacyNameString = legacyName == null ? "legacy_name" : legacyName.toString();

    Object descriptorFile = configs.get(PROTOBUF_DESCRIPTOR_SET);
    String descriptorFileString = descriptorFile == null ? PROTOBUF_DESCRIPTOR_SET_DEFAULT : descriptorFile.toString();

    Object protoTypeName = configs.get(PROTOBUF_TYPE);

    if (isInvalidConfiguration(protoTypeName, isKey)) {
      throw new ConnectException("Value converter must have a " + PROTOBUF_TYPE + " configured");
    }

    if (protoTypeName == null) {
      protobufData = null;
      return;
    }

    try {
      final DescriptorSetSchemaProvider provider = new DescriptorSetSchemaProvider(descriptorFileString);
      Descriptors.Descriptor rootMessageDescriptor = provider.getDescriptorForTypeName(protoTypeName.toString());
      protobufData = new ProtobufData(rootMessageDescriptor, legacyNameString);
    } catch(Exception e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (protobufData == null || schema == null || value == null) {
      return null;
    }

    return protobufData.fromConnectData(value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    if (protobufData == null || value == null) {
      return SchemaAndValue.NULL;
    }

    return protobufData.toConnectData(value);
  }
}
