package com.bakdata.commons;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Allows polling the boolean status of a specific operation
 */
public interface StatusProvider {

  /**
   * Check whether the operation was successful
   *
   * @return boolean value indicating that the operation was successful or not
   * @throws IOException if an error occurs retrieving the status
   */
  boolean success() throws IOException;

  /**
   * Poll status periodically until true or timeout reached.
   *
   * @param timeout time to wait for status
   * @param timeUnit time unit
   * @return true if successful, false if timeout reached before success
   * @throws IOException if an error polling status occurs {@link #success()}
   * @throws InterruptedException if thread is interrupted while waiting
   */
  default boolean waitForSuccess(long timeout, TimeUnit timeUnit)
      throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    while (!success()) {
      long delta = System.currentTimeMillis() - start;
      if (delta > timeout) {
        return false;
      }
      timeUnit.sleep(5L);
    }
    return true;
  }

}
