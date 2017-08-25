package com.bakdata.commons.serialization;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public abstract class SerializerTest<T> {

  @Parameter
  public T toSerialize;

  @Test
  public void test() throws SerializationException {
    Serializer<T> serializer = getSerializer();
    byte[] serialized = serializer.serialize(toSerialize);
    assertThat(serializer.deserialize(serialized), is(toSerialize));
  }

  protected abstract Serializer<T> getSerializer();
}
