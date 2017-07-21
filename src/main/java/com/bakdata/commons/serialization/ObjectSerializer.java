package com.bakdata.commons.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public enum ObjectSerializer implements Serializer<Object> {

  INSTANCE;

  @Override
  public Object deserialize(byte[] bytes) throws SerializationException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    try (ObjectInput in = new ObjectInputStream(bis)) {
      return in.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public byte[] serialize(Object t) throws SerializationException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(t);
      out.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

}
