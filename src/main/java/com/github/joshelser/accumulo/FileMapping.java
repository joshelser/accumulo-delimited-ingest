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
 * Defines the mapping of column in the delimited file to the Accumulo table schema.
 */
public interface FileMapping {

  /**
   * Returns the {@link Mapping} for the column with the given offset for this CSV file.
   */
  Mapping getMapping(int offset);

  /**
   * Returns the {@link RowMapping} for the CSV file.
   */
  RowMapping getRowMapping();

  /**
   * Returns the {@link ColumnMapping} for the CSV file at the current offset.
   */
  ColumnMapping getColumnMapping(int offset);

  /**
   * Returns the number of mappings for this CSV file.
   */
  int numMappings();
}