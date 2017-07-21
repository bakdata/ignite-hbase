package com.bakdata.commons.serialization;

import java.util.Objects;

public class ByteArraySerializer implements Serializer<byte[]> {

  private static final long serialVersionUID = 7466991002994033433L;

  @Override
  public byte[] deserialize(byte[] bytes) throws SerializationException {
    return bytes;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && this.getClass().equals(obj.getClass());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getClass());
  }

  @Override
  public byte[] serialize(byte[] bytes) throws SerializationException {
    return bytes;
  }
}
