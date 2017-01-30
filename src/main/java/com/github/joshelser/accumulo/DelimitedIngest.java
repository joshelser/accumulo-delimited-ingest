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

import java.io.IOException;
import java.nio.ByteBuffer;
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

/**
 * Ingests records read from files into an Accumulo table.
 */
public class DelimitedIngest implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(DelimitedIngest.class);

  public static final String ROW_ID = "!rowId";
  public static final char NEWLINE = '\n';
  public static final int INPUT_BUFFER_SIZE = 4096;

  private final DelimitedIngestArguments args;
  private final Configuration conf;

  public DelimitedIngest(DelimitedIngestArguments args) {
    this.args = requireNonNull(args);
    this.conf = new Configuration();
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

    ColumnMapping mapping = parseColumnMapping();
    BatchWriter writer;
    try {
      // TODO configurable BatchWriterConfig
      writer = conn.createBatchWriter(args.getTableName(), new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      LOG.error("Failed to create Accumulo BatchWriter", e);
      return ReturnCodes.BATCH_WRITER_CREATION_FAILURE;
    }

    for (Path path : filesToProcess) {
      try {
        processSinglePath(writer, mapping, path);
      } catch (MutationsRejectedException e) {
        LOG.error("Failed to write data to Accumulo", e);
        return ReturnCodes.DATA_WRITE_FAILURE;
      } catch (IOException e) {
        LOG.error("Failed to process " + path, e);
        return ReturnCodes.INPUT_PATH_PROCESSING;
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
      return ClientConfiguration.loadDefault();
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

  private ColumnMapping parseColumnMapping() {
    return null;
  }

  private void processSinglePath(BatchWriter writer, ColumnMapping mapping, Path p) throws IOException, MutationsRejectedException {
    final FileSystem fs = p.getFileSystem(conf);
    FSDataInputStream dis = fs.open(p, INPUT_BUFFER_SIZE);
    ByteBuffer buffer = ByteBuffer.allocate(INPUT_BUFFER_SIZE);
    dis.read(buffer);
    while (true) {
      int startingOffset = buffer.position();
      int length = buffer.limit();
      // Don't need to check 'remaining', length is guaranteed to be less than that.
      for (int offset = startingOffset; offset < length; offset += 2) {
        char currentCharacter = buffer.getChar(offset);
        if (NEWLINE == currentCharacter) {
          // Collected a "line", ingest it
          writer.addMutation(parseLine(mapping, buffer, startingOffset, offset));
          // prepare for the next line
          startingOffset = offset + 2;
        }
      }

      // Leftovers to read.
      // TODO what happens when the leftovers are larger than `INPUT_BUFFER_SIZE`
      if (startingOffset < length) {
        // Copy the leftover bytes out.
        byte[] leftovers = new byte[length - startingOffset];
        buffer.get(leftovers);
        // Reset the buffer
        buffer.reset();
        // Put the leftovers at the beginning of the buffer
        buffer.put(leftovers);

        // Fill the buffer with more data
        dis.read(buffer);
      }
    }
  }

  private Mutation parseLine(ColumnMapping mapping, ByteBuffer buffer, int begin, int end) {
    byte[] rowId = mapping.getRowId(buffer, begin, end);
    Mutation m = new Mutation(rowId);
    mapping.addColumns(m, buffer, begin, end);
    return m;
  }
  
  public static void main(String[] args) {
    // Delegate over to DelimitedIngestCliRunner
    DelimitedIngestCliRunner.main(args);
  }
}
