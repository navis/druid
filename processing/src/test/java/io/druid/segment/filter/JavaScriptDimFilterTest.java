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

package io.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 */
@RunWith(Parameterized.class)
public class JavaScriptDimFilterTest
{
  /*
   *  3 row values of three dimensions
   *
   *     foo  |  bar  |  baz
   *     ====================
   *     foo1 |  bar1 |  foo1
   *     foo2 |  foo3 |  foo3
   *     foo3 |  foo1 |  foo3
   */
  private static final Map<String, String[]> DIM_VALS = ImmutableMap.of(
      "foo", new String[]{"foo1", "foo2", "foo3"},
      "bar", new String[]{"bar1", "foo1", "foo3"},
      "baz", new String[]{"foo1", "foo3"}
  );

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new ConciseBitmapFactory()},
        new Object[]{new RoaringBitmapFactory()}
    );
  }

  public JavaScriptDimFilterTest(BitmapFactory bitmapFactory)
  {
    bitmapMap = new LinkedHashMap<>();
    final MutableBitmap mutableBitmapfoo1 = bitmapFactory.makeEmptyMutableBitmap();
    final MutableBitmap mutableBitmapfoo2 = bitmapFactory.makeEmptyMutableBitmap();
    final MutableBitmap mutableBitmapfoo3 = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmapfoo1.add(0);
    mutableBitmapfoo2.add(1);
    mutableBitmapfoo3.add(2);
    Map<String, ImmutableBitmap> foo = new LinkedHashMap<>();
    foo.put("foo1", bitmapFactory.makeImmutableBitmap(mutableBitmapfoo1));
    foo.put("foo2", bitmapFactory.makeImmutableBitmap(mutableBitmapfoo2));
    foo.put("foo3", bitmapFactory.makeImmutableBitmap(mutableBitmapfoo3));
    this.bitmapMap.put("foo", foo);

    final MutableBitmap mutableBitmapbar1 = bitmapFactory.makeEmptyMutableBitmap();
    final MutableBitmap mutableBitmapbar2 = bitmapFactory.makeEmptyMutableBitmap();
    final MutableBitmap mutableBitmapbar3 = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmapbar1.add(0);
    mutableBitmapbar2.add(2);
    mutableBitmapbar3.add(1);
    Map<String, ImmutableBitmap> bar = new LinkedHashMap<>();
    bar.put("bar1", bitmapFactory.makeImmutableBitmap(mutableBitmapbar1));
    bar.put("foo1", bitmapFactory.makeImmutableBitmap(mutableBitmapbar2));
    bar.put("foo3", bitmapFactory.makeImmutableBitmap(mutableBitmapbar3));
    this.bitmapMap.put("bar", bar);

    final MutableBitmap mutableBitmapbaz1 = bitmapFactory.makeEmptyMutableBitmap();
    final MutableBitmap mutableBitmapbaz2 = bitmapFactory.makeEmptyMutableBitmap();
    mutableBitmapbaz1.add(0);
    mutableBitmapbaz2.add(1);
    mutableBitmapbaz2.add(2);
    Map<String, ImmutableBitmap> baz = new LinkedHashMap<>();
    baz.put("foo1", bitmapFactory.makeImmutableBitmap(mutableBitmapbaz1));
    baz.put("foo3", bitmapFactory.makeImmutableBitmap(mutableBitmapbaz2));
    this.bitmapMap.put("baz", baz);

    this.factory = bitmapFactory;
  }

  private final BitmapFactory factory;
  private final Map<String, Map<String, ImmutableBitmap>> bitmapMap;

  private final BitmapIndexSelector BITMAP_INDEX_SELECTOR = new BitmapIndexSelector()
  {
    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      final String[] vals = DIM_VALS.get(dimension);
      return vals == null ? null : new ArrayIndexed<String>(vals, String.class);
    }

    @Override
    public int getNumRows()
    {
      return 3;
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return factory;
    }

    @Override
    public BitmapIndex getBitmapIndex(String dimension)
    {
      return null;
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String dimension, String value)
    {
      return bitmapMap.get(dimension).get(value);
    }

    @Override
    public ImmutableRTree getSpatialIndex(String dimension)
    {
      return null;
    }
  };
  private static final String javaScript = "function(dim1, dim2) { return dim1 === dim2 }";
  private static final String[] dimensions1 = {"foo", "bar"};
  private static final String[] dimensions2 = {"foo", "baz"};

  @Test
  public void testEmpty()
  {
    Filter javaScriptFilter = JavaScriptDimFilter.of(dimensions1, javaScript, null).toFilter();
    ImmutableBitmap immutableBitmap = javaScriptFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(0, immutableBitmap.size());
  }

  @Test
  public void testNormal()
  {
    Filter javaScriptFilter = JavaScriptDimFilter.of(dimensions2, javaScript, null).toFilter();
    ImmutableBitmap immutableBitmap = javaScriptFilter.getBitmapIndex(BITMAP_INDEX_SELECTOR);
    Assert.assertEquals(2, immutableBitmap.size());
  }

  @Test
  public void testOr()
  {
    Assert.assertEquals(
        2,
        DimFilters.or(
            JavaScriptDimFilter.of(dimensions2, javaScript, null)
        ).toFilter().getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        2,
        DimFilters.or(
            JavaScriptDimFilter.of(dimensions1, javaScript, null),
            JavaScriptDimFilter.of(dimensions2, javaScript, null)
        ).toFilter().getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }

  @Test
  public void testAnd()
  {
    Assert.assertEquals(
        2,
        DimFilters.or(
            JavaScriptDimFilter.of(dimensions2, javaScript, null)
        ).toFilter().getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        0,
        DimFilters.and(
            JavaScriptDimFilter.of(dimensions1, javaScript, null),
            JavaScriptDimFilter.of(dimensions2, javaScript, null)
        ).toFilter().getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }

  @Test
  public void testNot()
  {
    Assert.assertEquals(
        2, DimFilters.or(
            JavaScriptDimFilter.of(dimensions2, javaScript, null)
        ).toFilter().getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );

    Assert.assertEquals(
        1,
        DimFilters.not(
            JavaScriptDimFilter.of(dimensions2, javaScript, null)
        ).toFilter().getBitmapIndex(BITMAP_INDEX_SELECTOR).size()
    );
  }
}
