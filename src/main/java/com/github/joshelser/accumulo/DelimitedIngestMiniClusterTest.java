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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelimitedIngestMiniClusterTest {
  private static final Logger LOG = LoggerFactory.getLogger(DelimitedIngestMiniClusterTest.class);
  private static MiniAccumuloClusterImpl MAC;
  private static FileSystem FS;
  private static final String INSTANCE_NAME = "accumulo";
  private static final String ROOT_PASSWORD = "password";
  private static final int NUM_ROWS = 100;
  private static final int NUM_COLUMNS = 10;
  private static final char COMMA = ',';
  private static final String NEWLINE = "\n";

  public static DelimitedIngestArguments ARGS;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    File targetDir = new File(System.getProperty("user.dir"), "target");
    File macDir = new File(targetDir, DelimitedIngestMiniClusterTest.class.getSimpleName() + "_cluster");
    if (macDir.exists()) {
      FileUtils.deleteDirectory(macDir);
    }
    MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(macDir, ROOT_PASSWORD);
    config.setNumTservers(1);
    config.setInstanceName(INSTANCE_NAME);
    config.setSiteConfig(Collections.singletonMap("fs.file.impl", RawLocalFileSystem.class.getName()));
    config.useMiniDFS(true);
    MAC = new MiniAccumuloClusterImpl(config);
    MAC.start();
    FS = FileSystem.get(MAC.getMiniDfs().getConfiguration(0));

    ARGS = new DelimitedIngestArguments();
    ARGS.setUsername("root");
    ARGS.setPassword(ROOT_PASSWORD);
    ARGS.setInstanceName(INSTANCE_NAME);
    ARGS.setZooKeepers(MAC.getZooKeepers());
    ARGS.setConfiguration(MAC.getMiniDfs().getConfiguration(0));
  }

  @AfterClass
  public static void stopMiniCluster() throws Exception {
    if (null != MAC) {
      MAC.stop();
    }
  }

  @Test
  public void testIngest() throws Exception {
    final File csvData = generateCsvData();
    FS.copyFromLocalFile(new Path(csvData.getAbsolutePath()), new Path(csvData.getName()));

    // Create a table
    final String tableName = testName.getMethodName();
    Connector conn = MAC.getConnector("root", new PasswordToken(ROOT_PASSWORD));
    conn.tableOperations().create(tableName);

    ARGS.setTableName(tableName);
    ARGS.setInput(Collections.singletonList(csvData.getName()));
    ARGS.setColumnMapping(":rowId,f1:q2,f1:q3,f1:q3,f1:q4,f1:q5,f1:q6,f1:q7,f1:q8,f1:q9");

    DelimitedIngest ingester = new DelimitedIngest(ARGS);
    assertEquals(ReturnCodes.NORMAL, ingester.call().intValue());
  }

  private File generateCsvData() throws IOException {
    final File targetDir = new File(System.getProperty("user.dir"), "target");
    final File csvData = new File(targetDir, testName.getMethodName() + "_data.csv");
    final FileOutputStream fos;
    try {
      fos = new FileOutputStream(csvData);
    } catch (FileNotFoundException fnfe) {
      LOG.error("Could not create data", fnfe);
      Assert.fail("Could not create data");
      return null;
    }

    try {
      StringBuilder sb = new StringBuilder(64);
      for (int i = 0; i < NUM_ROWS; i++) {
        for (int j = 0; j < NUM_COLUMNS; j++) {
          if (sb.length() > 0) {
            sb.append(COMMA);
          }
          sb.append("column").append(i).append("_").append(j);
        }
        sb.append(NEWLINE);
        fos.write(sb.toString().getBytes(UTF_8));
        sb.setLength(0);
      }
  
      return csvData;
    } finally {
      if (null != fos) {
        fos.close();
      }
    }
  }
}
