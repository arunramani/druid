/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumnsTest;
import org.apache.druid.segment.CursorFactory;

import java.util.function.Function;

public class CursorFactoryRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public CursorFactoryRowsAndColumnsTest()
  {
    super(CursorFactoryRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, CursorFactoryRowsAndColumns> MAKER =
      CursorFactoryRowsAndColumnsTest::buildFrame;

  private static CursorFactoryRowsAndColumns buildFrame(MapOfColumnsRowsAndColumns input)
  {
    try (ColumnBasedFrameRowsAndColumns fRAC = ColumnBasedFrameRowsAndColumnsTest.buildFrame(input)) {
      return new CursorFactoryRowsAndColumns(fRAC.as(CursorFactory.class));
    }
  }
}
