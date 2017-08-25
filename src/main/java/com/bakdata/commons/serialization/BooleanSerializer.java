package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

/**
 * Type-specific {@link Serializer} for {@link Boolean}
 */
public enum BooleanSerializer implements Serializer<Boolean> {

  INSTANCE;

  @Override
  public Boolean deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).get() == 1;
  }

  @Override
  public byte[] serialize(Boolean t) {
    return ByteBuffer.allocate(Byte.BYTES).put((byte) (t ? 1 : 0)).array();
  }

}
