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

/**
 * A collection of return codes set by {@link DelimitedIngestCliRunner}.
 */
public interface ReturnCodes {
  /**
   * The ingester returned normally.
   */
  int NORMAL = 0;
  /**
   * Argument parsing was not successful.
   */
  int ARGUMENT_PARSING_FAILED = 1;
  /**
   * The user requested the help argument. It should be printed, and then execution terminated.
   */
  int HELP_REQUESTED = 2;
  /**
   * An unhandled exception was thrown and execution was terminated.
   */
  int UNHANDLED_FAILURE = 3;

  int CLIENT_CONFIG_PARSE_FAILURE = 4;

  int AUTHENTICATION_FAILURE = 5;

  int INPUT_PATH_PARSING = 6;

  int INPUT_PATH_PROCESSING = 7;

  int DATA_WRITE_FAILURE = 8;

  int BATCH_WRITER_CREATION_FAILURE = 9;
}
