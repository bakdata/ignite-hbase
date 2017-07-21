package com.bakdata.commons.serialization;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class FloatSerializer implements Serializer<Float> {

  private static final long serialVersionUID = -7249683509663812457L;

  @Override
  public Float deserialize(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
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
  public byte[] serialize(Float t) {
    return ByteBuffer.allocate(Float.BYTES).putFloat(t).array();
  }

}
