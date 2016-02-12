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

package io.druid.indexing.test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;

public class TestDataSegmentPusher implements DataSegmentPusher, SegmentHandoffNotifierFactory
{
  private final Set<DataSegment> pushedSegments = Sets.newConcurrentHashSet();

  @Override
  public String getPathForHadoop(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataSegment push(File file, DataSegment segment) throws IOException
  {
    pushedSegments.add(segment);
    return segment;
  }

  public Set<DataSegment> getPushedSegments()
  {
    return ImmutableSet.copyOf(pushedSegments);
  }

  @Override
  public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
  {
    return new SegmentHandoffNotifier()
    {
      @Override
      public boolean registerSegmentHandoffCallback(
          SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
      )
      {
        for (int i = 0; i < 10; i++) {
          for (DataSegment segment : getPushedSegments()) {
            if (segment.getVersion().equals(descriptor.getVersion()) && segment.getInterval().equals(descriptor.getInterval())) {
              handOffRunnable.run();
              return true;
            }
          }
          try {
            Thread.sleep(1000);
          }
          catch (InterruptedException e) {
            return false;
          }
        }
        return false;
      }

      @Override
      public void start()
      {

      }

      @Override
      public void stop()
      {

      }
    };
  }
}
