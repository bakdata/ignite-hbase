package com.bakdata.commons.serialization;

import java.io.IOException;

public class SerializationException extends IOException {

  private static final long serialVersionUID = -5449047926851214782L;

  public SerializationException() {

  }

  public SerializationException(String message) {
    super(message);
  }

  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SerializationException(Throwable cause) {
    super(cause);
  }
}
