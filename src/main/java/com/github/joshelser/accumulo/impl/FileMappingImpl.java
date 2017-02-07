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

import java.util.ArrayList;
import java.util.List;

import com.github.joshelser.accumulo.ColumnMapping;
import com.github.joshelser.accumulo.DelimitedIngest;
import com.github.joshelser.accumulo.FileMapping;
import com.github.joshelser.accumulo.Mapping;
import com.github.joshelser.accumulo.RowMapping;

public class FileMappingImpl implements FileMapping {
  private List<Mapping> mappings;
  private RowMapping rowMapping;

  public FileMappingImpl(String config) {
    parse(config);
  }

  private void parse(String config) {
    String[] entries = config.split(",");
    mappings = new ArrayList<>(entries.length);
    int rowIdOffset = -1;
    int i = 0;
    for (String entry : entries) {
      entry = entry.trim();
      Mapping mapping;
      if (DelimitedIngest.ROW_ID.equals(entry)) {
        rowIdOffset = i;
        rowMapping = new RowMappingImpl(rowIdOffset);
        mapping = rowMapping;
      } else {
        mapping = new ColumnMappingImpl(entry);
      }
      mappings.add(mapping);
      i++;
    }
    if (null == rowMapping) {
      throw new IllegalArgumentException("Did not find rowId mapping in '" + config + "'");
    }
  }

  @Override
  public Mapping getMapping(int offset) {
    return mappings.get(offset);
  }

  @Override
  public RowMapping getRowMapping() {
    return rowMapping;
  }

  @Override
  public ColumnMapping getColumnMapping(int offset) {
    Mapping mapping = mappings.get(offset);
    if (!(mapping instanceof ColumnMapping)) {
      throw new IllegalArgumentException("Mapping at offset " + offset + " is not a ColumnMapping");
    }
    return (ColumnMapping) mapping;
  }

  @Override
  public int numMappings() {
    return mappings.size();
  }
}
