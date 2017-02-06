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

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * POJO encapsulating command-line arguments for {@link DelimitedIngest}.
 */
public class DelimitedIngestArguments {
  private static class ColumnMappingValidator implements IValueValidator<String> {
    @Override
    public void validate(String name, String value) throws ParameterException {
      String[] elements = value.split(",");
      boolean sawRowId = false;
      for (String element : elements) {
        if (DelimitedIngest.ROW_ID.equals(element)) {
          if (sawRowId) {
            throw new ParameterException("Saw multiple instance of '" + DelimitedIngest.ROW_ID + "' in the column mapping.");
          }
          sawRowId = true;
        }
      }
      if (!sawRowId) {
        throw new ParameterException("One element in the column mapping must be '" + DelimitedIngest.ROW_ID + "', but found none");
      }
    }
  }

  @Parameter(names = {"-h", "--help", "-help"}, help = true)
  private boolean help = false;

  @Parameter(names = {"-c", "--config"}, description = "Path to the Accumulo client configuration file")
  private String clientConfigPath;

  @Parameter(names = {"-u", "--username"}, description = "The user to connect to Accumulo as", required = true)
  private String username;

  @Parameter(names = {"-p", "--password"}, description = "The password for the Accumulo user", password = true)
  private String password;

  @Parameter(names = {"--instance"}, description = "The Accumulo instance name")
  private String instanceName;

  @Parameter(names = {"-zk", "--zookeepers"}, description = "ZooKeeper servers used by Accumulo")
  private String zooKeepers;

  @Parameter(names = {"-i", "--input"}, description = "The input data (files, directories, URIs)", required = true, variableArity = true)
  private List<String> input;

  @Parameter(names = {"-t", "--table"}, description = "The Accumulo table to ingest data into", required = true)
  private String tableName;

  @Parameter(names = {"-q", "--quotedValues"}, description = "Are the values on each line quoted (default=false)")
  private boolean quotedValues = false;

  @Parameter(names = {"-c", "--columnMapping"}, description = "Comma-separated value of the mapping to the Accumulo schema", required = true,
      validateValueWith = ColumnMappingValidator.class)
  private String columnMapping = null;

  private Configuration conf = new Configuration();

  public boolean isHelp() {
    return help;
  }

  public void setHelp(boolean help) {
    this.help = help;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  public String getZooKeepers() {
    return zooKeepers;
  }

  public void setZooKeepers(String zooKeepers) {
    this.zooKeepers = zooKeepers;
  }

  public List<String> getInput() {
    return input;
  }

  public void setInput(List<String> input) {
    this.input = input;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean isQuotedValues() {
    return quotedValues;
  }

  public void setQuotedValues(boolean quotedValues) {
    this.quotedValues = quotedValues;
  }

  public String getClientConfigPath() {
    return clientConfigPath;
  }

  public void setClientConfigPath(String clientConfigPath) {
    this.clientConfigPath = clientConfigPath;
  }

  public String getColumnMapping() {
    return columnMapping;
  }

  public void setColumnMapping(String columnMapping) {
    this.columnMapping = columnMapping;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public void setConfiguration(Configuration conf) {
    this.conf = conf;
  }
}
