package com.bakdata.commons.serialization;

/**
 * Type-specific {@link Serializer} for {@code byte[]}
 */
public enum ByteArraySerializer implements Serializer<byte[]> {

  INSTANCE;

  @Override
  public byte[] deserialize(byte[] bytes) throws SerializationException {
    return bytes;
  }

  @Override
  public byte[] serialize(byte[] bytes) throws SerializationException {
    return bytes;
  }
}
