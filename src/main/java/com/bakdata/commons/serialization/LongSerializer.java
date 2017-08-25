package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

/**
 * Type-specific {@link Serializer} for {@link Long}
 */
public enum LongSerializer implements Serializer<Long> {

  INSTANCE;

  @Override
  public Long deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  @Override
  public byte[] serialize(Long t) {
    return ByteBuffer.allocate(Long.BYTES).putLong(t).array();
  }

}
