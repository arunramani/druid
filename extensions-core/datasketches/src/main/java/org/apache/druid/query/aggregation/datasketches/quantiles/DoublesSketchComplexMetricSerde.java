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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.google.common.primitives.Doubles;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

public class DoublesSketchComplexMetricSerde extends ComplexMetricSerde
{

  private static final DoublesSketchObjectStrategy STRATEGY = new DoublesSketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return DoublesSketchModule.DOUBLES_SKETCH;
  }

  @Override
  public ObjectStrategy<DoublesSketch> getObjectStrategy()
  {
    return STRATEGY;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      private static final int MIN_K = 2; // package one input value into the smallest sketch

      @Override
      public Class<?> extractedClass()
      {
        return DoublesSketch.class;
      }

      @Override
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object instanceof String) { // everything is a string during ingestion
          String objectString = (String) object;
          // Autodetection of the input format: empty string, number, or base64 encoded sketch
          // A serialized DoublesSketch, as currently implemented, always has 0 in the first 6 bits.
          // This corresponds to "A" in base64, so it is not a digit
          final Double doubleValue;
          if (objectString.isEmpty()) {
            return DoublesSketchOperations.EMPTY_SKETCH;
          } else if ((doubleValue = Doubles.tryParse(objectString)) != null) {
            UpdateDoublesSketch sketch = DoublesSketch.builder().setK(MIN_K).build();
            sketch.update(doubleValue);
            return sketch;
          }
        } else if (object instanceof Number) { // this is for reindexing
          UpdateDoublesSketch sketch = DoublesSketch.builder().setK(MIN_K).build();
          sketch.update(((Number) object).doubleValue());
          return sketch;
        }

        if (object == null || object instanceof DoublesSketch || object instanceof Memory) {
          return object;
        }
        return DoublesSketchOperations.deserializeSafe(object);
      }
    };
  }
}
