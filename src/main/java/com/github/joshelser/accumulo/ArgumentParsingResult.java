/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.accumulo;

import static java.util.Objects.requireNonNull;

/**
 * The result of parsing the arguments to {@link DelimitedIngest}.
 */
public class ArgumentParsingResult {

  public static enum Result {
    // Successfully parsed the arguments
    SUCCESSFUL,
    // Failed to parse
    FAILED,
    // Did not fail, but should not proceed
    TERMINATE,
  }

  private final Result result;
  private final DelimitedIngestArguments args;
  private final Exception e;

  private ArgumentParsingResult(Result result, DelimitedIngestArguments args, Exception e) {
    this.result = result;
    this.args = args;
    this.e = e;
  }

  /**
   * Returns the result of parsing the arguments.
   */
  public Result getResult() {
    return result;
  }

  /**
   * Returns the {@link DelimitedIngestArguments}. Will be null when {@link #getResult()}
   * is {@code null}.
   */
  public DelimitedIngestArguments getArgs() {
    return args;
  }

  /**
   * Returns the {@link Exception} thrown by argument parsing. Will be {@code null} unless
   * {@link #getResult()} is {@code FAILED}.
   */
  public Exception getException() {
    return e;
  }

  /**
   * Creates an object to represent that argument parsing was successful.
   */
  public static ArgumentParsingResult successful(DelimitedIngestArguments args) {
    return new ArgumentParsingResult(Result.SUCCESSFUL, requireNonNull(args), null);
  }

  /**
   * Creates an object to represent that argument parsing failed.
   */
  public static ArgumentParsingResult failed(Exception e) {
    return new ArgumentParsingResult(Result.FAILED, null, e);
  }

  /**
   * Creates an object to represent that argument parsing succeeded but control should still terminate.
   */
  public static ArgumentParsingResult terminate(DelimitedIngestArguments args) {
    return new ArgumentParsingResult(Result.TERMINATE, requireNonNull(args), null);
  }
}
