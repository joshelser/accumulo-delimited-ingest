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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Mutation;

import com.github.joshelser.accumulo.DelimitedIngest;
import com.github.joshelser.accumulo.RowMapping;

public class RowMappingImpl implements RowMapping {
  private final int logicalOffset;

  public RowMappingImpl(int logicalOffset) {
    this.logicalOffset = logicalOffset;
  }

  @Override
  public Mutation getRowId(CharBuffer buffer, int start, int end) {
    try {
      int columnOffset = 0;
      // Construct the Mutation
      for (int offset = start; offset < end; offset++) {
        if (DelimitedIngest.COMMA == buffer.get(offset)) {
          if (columnOffset == logicalOffset) {
            // Set the start and end on the charbuffer
            buffer.position(start);
            buffer.limit(offset);
            // Encode that back into bytes
            ByteBuffer bb = StandardCharsets.UTF_8.encode(buffer);
            // Make a copy for Mutation
            byte[] bytes = new byte[bb.limit() - bb.position()];
            bb.get(bytes);
            return new Mutation(bytes);
          } else {
            columnOffset++;
          }
        }
      }
      throw new IllegalArgumentException("Could not find mapping for rowId in row");
    } finally {
      buffer.position(start);
    }
  }

  @Override
  public int getLogicalOffset() {
    return logicalOffset;
  }
}
