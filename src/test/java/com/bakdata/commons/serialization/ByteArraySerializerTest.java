package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class ByteArraySerializerTest extends SerializerTest<byte[]> {

  @Parameters
  public static Collection<byte[]> data() {
    return Arrays.asList(new byte[]{(byte) 0}, new byte[]{(byte) 2, (byte) -3}, new byte[]{});
  }

  @Override
  protected Serializer<byte[]> getSerializer() {
    return ByteArraySerializer.INSTANCE;
  }
}
