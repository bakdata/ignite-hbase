package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

/**
 * Type-specific {@link Serializer} for {@link Float}
 */
public enum FloatSerializer implements Serializer<Float> {

  INSTANCE;

  @Override
  public Float deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
  }

  @Override
  public byte[] serialize(Float t) {
    return ByteBuffer.allocate(Float.BYTES).putFloat(t).array();
  }

}
