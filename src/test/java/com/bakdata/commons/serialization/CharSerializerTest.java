package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class CharSerializerTest extends SerializerTest<Character> {

  @Parameters
  public static Collection<Character> data() {
    return Arrays.asList('a', 'b');
  }

  @Override
  protected Serializer<Character> getSerializer() {
    return CharSerializer.INSTANCE;
  }
}
