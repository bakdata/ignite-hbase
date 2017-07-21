package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

public enum ByteSerializer implements Serializer<Byte> {

  INSTANCE;

  @Override
  public Byte deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).get();
  }

  @Override
  public byte[] serialize(Byte t) {
    return ByteBuffer.allocate(Byte.BYTES).put(t).array();
  }

}
