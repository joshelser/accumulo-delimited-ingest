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
package com.github.joshelser.accumulo.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Mutation;

import com.github.joshelser.accumulo.ColumnMapping;

public class ColumnMappingImpl implements ColumnMapping {
  private final byte[] family;
  private final byte[] qualifier;

  public ColumnMappingImpl(String columnDef) {
    int offset = columnDef.indexOf(':');
    if (offset == -1) {
      family = columnDef.getBytes(StandardCharsets.UTF_8);
      qualifier = new byte[0];
    } else {
      family = columnDef.substring(0, offset).getBytes(StandardCharsets.UTF_8);
      qualifier = columnDef.substring(offset + 1).getBytes(StandardCharsets.UTF_8);
    }
  }

  @Override
  public void addColumns(Mutation m, String columnValue) {
    m.put(family, qualifier, columnValue.getBytes(UTF_8));
  }
}
