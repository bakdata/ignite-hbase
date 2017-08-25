package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

/**
 * Type-specific {@link Serializer} for {@link Short}
 */
public enum ShortSerializer implements Serializer<Short> {

  INSTANCE;

  @Override
  public Short deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getShort();
  }

  @Override
  public byte[] serialize(Short t) {
    return ByteBuffer.allocate(Short.BYTES).putShort(t).array();
  }

}
