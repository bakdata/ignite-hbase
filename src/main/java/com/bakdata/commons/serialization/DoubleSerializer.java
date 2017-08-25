package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

/**
 * Type-specific {@link Serializer} for {@link Double}
 */
public enum DoubleSerializer implements Serializer<Double> {

  INSTANCE;

  @Override
  public Double deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getDouble();
  }

  @Override
  public byte[] serialize(Double t) {
    return ByteBuffer.allocate(Double.BYTES).putDouble(t).array();
  }

}
