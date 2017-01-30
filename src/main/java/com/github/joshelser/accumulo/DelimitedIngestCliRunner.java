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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runner class for DelimitedIngest which sets return codes on the application.
 */
public class DelimitedIngestCliRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DelimitedIngestCliRunner.class);

  public static void main(String[] args) {
    ArgumentParser parser = new ArgumentParser();
    ArgumentParsingResult parseResult = parser.parse(args);
    switch (parseResult.getResult()) {
      case FAILED:
        System.exit(ReturnCodes.ARGUMENT_PARSING_FAILED);
        return;
      case TERMINATE:
        System.exit(ReturnCodes.HELP_REQUESTED);
        return;
      case SUCCESSFUL:
        // pass
        break;
      default:
        IllegalStateException e = new IllegalStateException("Should not have reached this point.");
        LOG.error("Programming error", e);
        throw e;
    }

    // If we made it here, we were successful and can continue
    DelimitedIngest ingester = new DelimitedIngest(parseResult.getArgs());
    // Fail-safe
    int returnCode = ReturnCodes.UNHANDLED_FAILURE;
    try {
      returnCode = ingester.call();
    } catch (Exception e) {
      LOG.error("An unhandled exception was caught during ingest.", e);
    } finally {
      System.exit(returnCode);
    }
  }
}
