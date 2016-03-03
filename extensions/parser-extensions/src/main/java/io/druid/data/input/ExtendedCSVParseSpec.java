/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.TimestampSpec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ExtendedCSVParseSpec extends CSVParseSpec
{
  private final int[] skipIndices;
  private final Splitter splitter;
  private final String[] columnNames;

  @JsonCreator
  public ExtendedCSVParseSpec(
      @JsonProperty("timestampSpec") final TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") final DimensionsSpec dimensionsSpec,
      @JsonProperty("listDelimiter") final String listDelimiter,
      @JsonProperty("columns") final List<String> columns,
      @JsonProperty("skipSplitColumns") final List<String> skipSplitColumns
  )
  {
    super(timestampSpec, dimensionsSpec, listDelimiter, columns);
    columnNames = getColumns().toArray(new String[0]);
    skipIndices = toSkipIndices(columns, skipSplitColumns);
    splitter = listDelimiter == null ? null : Splitter.on(listDelimiter);
  }

  private int[] toSkipIndices(List<String> columns, List<String> skipSplitColumns)
  {
    if (skipSplitColumns == null || skipSplitColumns.isEmpty()) {
      return new int[0];
    }
    List<Integer> indices = Lists.newArrayListWithCapacity(skipSplitColumns.size());
    for (String skipField : skipSplitColumns) {
      int index = columns.indexOf(skipField);
      if (index >= 0) {
        indices.add(index);
      }
    }
    Collections.sort(indices);
    return Ints.toArray(indices);
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new Parser()
    {
      private final String delimiter = getListDelimiter();
      private final au.com.bytecode.opencsv.CSVParser parser = new au.com.bytecode.opencsv.CSVParser();

      @Override
      public Map<String, Object> parse(final String input)
      {
        LinkedHashMap<String, Object> parsed = Maps.newLinkedHashMap();
        try {
          String[] values = parser.parseLine(input);
          for (int i = 0, j = 0; i < values.length; i++) {
            if (splitter == null ||
                j < skipIndices.length && i == skipIndices[j++] ||
                !values[i].contains(delimiter)) {
              parsed.put(columnNames[i], ParserUtils.nullEmptyStringFunction.apply(values[i]));
              continue;
            }
            parsed.put(
                columnNames[i], Lists.newArrayList(
                    Iterables.transform(
                        splitter.split(values[i]), ParserUtils.nullEmptyStringFunction
                    )
                )
            );
          }
        }
        catch (Exception e) {
          throw new ParseException(e, "Unable to parse row [%s]", input);
        }
        return parsed;
      }

      @Override
      public List<String> getFieldNames()
      {
        return getColumns();
      }

      @Override
      public void setFieldNames(Iterable fieldNames)
      {
        throw new UnsupportedOperationException("setFieldNames");
      }
    };
  }
}
