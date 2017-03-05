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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.joshelser.accumulo.impl.FileMappingImpl;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * Ingests records read from files into an Accumulo table.
 */
public class DelimitedIngest implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(DelimitedIngest.class);

  public static final String ROW_ID = "!rowId";
  public static final char NEWLINE = '\n';
  public static final int INPUT_BUFFER_SIZE = 4096;
  public static final char COMMA = ',';

  private final DelimitedIngestArguments args;
  private final Configuration conf;

  public DelimitedIngest(DelimitedIngestArguments args) {
    this.args = requireNonNull(args);
    this.conf = args.getConfiguration();
  }

  public Integer call() {
    Connector conn;
    try {
      conn = getConnector();
    } catch (ConfigurationException e) {
      LOG.error("Caught exception parsing the Accumulo client configuration", e);
      return ReturnCodes.CLIENT_CONFIG_PARSE_FAILURE;
    } catch (AccumuloException | AccumuloSecurityException e) {
      LOG.error("Failed to authenticate with Accumulo", e);
      return ReturnCodes.AUTHENTICATION_FAILURE;
    }

    List<Path> filesToProcess;
    try {
      filesToProcess = convertInputToPaths();
    } catch (IOException e) {
      LOG.error("Failed to parse input paths", e);
      return ReturnCodes.INPUT_PATH_PARSING;
    }
    if (filesToProcess.isEmpty()) {
      // Not expected, but not necessarily failure either.
      LOG.info("No files provided, nothing to do.");
      return 0;
    }

    FileMapping mapping = parseColumnMapping();
    BatchWriter writer;
    try {
      // TODO configurable BatchWriterConfig
      writer = conn.createBatchWriter(args.getTableName(), new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      LOG.error("Failed to create Accumulo BatchWriter", e);
      return ReturnCodes.BATCH_WRITER_CREATION_FAILURE;
    }

    CsvParserSettings settings = new CsvParserSettings();
    settings.setColumnReorderingEnabled(false);
    settings.setSkipEmptyLines(true);
    settings.setHeaderExtractionEnabled(false);
    settings.setInputBufferSize(INPUT_BUFFER_SIZE);
    CsvParser parser = new CsvParser(settings);

    try {
      for (Path path : filesToProcess) {
        try {
          processSinglePathWithByteBuffer(writer, mapping, path, parser);
          writer.flush();
        } catch (MutationsRejectedException e) {
          LOG.error("Failed to write data to Accumulo", e);
          return ReturnCodes.DATA_WRITE_FAILURE;
        } catch (IOException e) {
          LOG.error("Failed to process " + path, e);
          return ReturnCodes.INPUT_PATH_PROCESSING;
        }
      }
    } finally {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        // Shouldn't ever actually happen since we flush()'ed above.
        LOG.error("Failed to write data to Accumulo", e);
        return ReturnCodes.DATA_WRITE_FAILURE;
      }
    }

    return 0;
  }

  private Connector getConnector() throws ConfigurationException, AccumuloException, AccumuloSecurityException {
    ClientConfiguration clientConfig = getClientConfig();
    ZooKeeperInstance zki = new ZooKeeperInstance(clientConfig);
    return zki.getConnector(args.getUsername(), new PasswordToken(args.getPassword()));
  }

  private ClientConfiguration getClientConfig() throws ConfigurationException {
    String path = args.getClientConfigPath();
    if (null == path) {
      LOG.trace("Client configuration path not provided, loading default.");
      ClientConfiguration clientConf = ClientConfiguration.loadDefault();
      if (null != args.getInstanceName()) {
        clientConf = clientConf.withInstance(args.getInstanceName());
      }
      if (null != args.getZooKeepers()) {
        clientConf = clientConf.withZkHosts(args.getZooKeepers());
      }
      return clientConf;
    }

    return new ClientConfiguration(path);
  }

  private List<Path> convertInputToPaths() throws IOException {
    List<String> inputs = args.getInput();
    List<Path> paths = new ArrayList<>(inputs.size());
    FileSystem fs = FileSystem.get(conf);
    for (String input : inputs) {
      Path p = new Path(input);
      FileStatus fstat = fs.getFileStatus(p);
      if (fstat.isFile()) {
        paths.add(p);
      } else if (fstat.isDirectory()) {
        for (FileStatus child : fs.listStatus(p)) {
          if (child.isFile()) {
            paths.add(child.getPath());
          }
        }
      } else {
        throw new IllegalStateException("Unable to handle that which is not file nor directory: " + p);
      }
    }
    return paths;
  }

  private FileMapping parseColumnMapping() {
    return new FileMappingImpl(args.getColumnMapping());
  }

  private void processSinglePathWithByteBuffer(BatchWriter writer, FileMapping mapping, Path p, CsvParser parser) throws IOException, MutationsRejectedException {
    final FileSystem fs = p.getFileSystem(conf);
    FSDataInputStream dis = fs.open(p, INPUT_BUFFER_SIZE);
    InputStreamReader reader = new InputStreamReader(dis, UTF_8);
    try {
      parser.beginParsing(reader);
      String[] line = null;
      while ((line = parser.parseNext()) != null) {
        writer.addMutation(parseLine(mapping, line));
      }
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
  }

  private Mutation parseLine(FileMapping mapping, String[] line) {
    RowMapping rowMapping = mapping.getRowMapping();
    // Construct the Mutation
    Mutation mutation = rowMapping.getRowId(line);
    int rowOffset = rowMapping.getLogicalOffset();
    assert null != mutation;

    // Build the Mutation - each "column" in the line of data
    for (int logicalOffset = 0; logicalOffset < mapping.numMappings(); logicalOffset++) {
      if (logicalOffset == rowOffset) {
        continue;
      }
      // Avoid calling getColumnMapping for the rowId offset
      ColumnMapping colMapping = mapping.getColumnMapping(logicalOffset);
      colMapping.addColumns(mutation, line[logicalOffset]);
    }

    return mutation;
  }
  
  public static void main(String[] args) {
    // Delegate over to DelimitedIngestCliRunner
    DelimitedIngestCliRunner.main(args);
  }
}
