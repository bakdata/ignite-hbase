package com.bakdata.commons.serialization;

/**
 * Type-specific {@link Serializer} for {@link String}
 */
public enum StringSerializer implements Serializer<String> {

  INSTANCE;

  @Override
  public String deserialize(byte[] bytes) {
    return new String(bytes);
  }

  @Override
  public byte[] serialize(String t) {
    return t.getBytes();
  }

}
