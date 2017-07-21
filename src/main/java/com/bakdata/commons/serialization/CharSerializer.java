package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class CharSerializer implements Serializer<Character> {

  private static final long serialVersionUID = -8040240659489903885L;

  @Override
  public Character deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getChar();
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
  public byte[] serialize(Character t) {
    return ByteBuffer.allocate(Character.BYTES).putChar(t).array();
  }

}
