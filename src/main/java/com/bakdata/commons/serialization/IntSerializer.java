package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

public enum IntSerializer implements Serializer<Integer> {

  INSTANCE;

  @Override
  public Integer deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  @Override
  public byte[] serialize(Integer t) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(t).array();
  }

}
