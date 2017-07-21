package com.bakdata.commons.serialization;

import java.io.Serializable;

/**
 * <p> Interface to take care of serializing and deserializing objects of class {@code T} from and
 * to {@code byte[]}. </p>
 *
 * <p>It is recommended to implement the {@code equals()} method.</p>
 *
 * @param <T> Class which can be serialized and deserialized by this class
 */
public interface Serializer<T> extends Serializable {

  /**
   * Deserializes an object of class {@code T} from a {@code byte[]}
   *
   * @param bytes raw bytes representing an object of class {@code T}
   * @return deserialized object
   * @throws SerializationException if deserialization fails
   */
  T deserialize(byte[] bytes) throws SerializationException;

  /**
   * Serialize an object of class {@code T} to a {@code byte[]}
   *
   * @param t object of class {@code T} to be serialized
   * @return raw bytes representing @{code t}
   * @throws SerializationException if serialization fails
   */
  byte[] serialize(T t) throws SerializationException;

}
