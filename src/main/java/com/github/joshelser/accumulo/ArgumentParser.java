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

import com.beust.jcommander.JCommander;

/**
 * Class which handles parsing argument parsing 
 */
public class ArgumentParser {

  public ArgumentParsingResult parse(String[] args) {
    DelimitedIngestArguments argsPojo = new DelimitedIngestArguments();
    try {
      JCommander jcommander = new JCommander(argsPojo, args);

      // If the user requested help, print it and then signal to terminate the app.
      if (argsPojo.isHelp()) {
        jcommander.usage();
        return ArgumentParsingResult.terminate(argsPojo);
      }

      return ArgumentParsingResult.successful(argsPojo);
    } catch (Exception e) {
      return ArgumentParsingResult.failed(e);
    }
  }
}
