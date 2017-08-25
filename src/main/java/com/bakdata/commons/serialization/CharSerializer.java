package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;

/**
 * Type-specific {@link Serializer} for {@link Character}
 */
public enum CharSerializer implements Serializer<Character> {

  INSTANCE;

  @Override
  public Character deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getChar();
  }

  @Override
  public byte[] serialize(Character t) {
    return ByteBuffer.allocate(Character.BYTES).putChar(t).array();
  }

}
