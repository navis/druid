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

package io.druid.segment.incremental;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClient;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.indexing.test.TestDataSegmentPusher;
import io.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 */
public class IncrementalIndexPerfTest
{

  final TestUtils testUtils = new TestUtils();

  private DataSchema toDataSchema(ParseSpec parseSpec, AggregatorFactory[] aggregators, GranularitySpec granularitySpec)
  {
    final Map<String, Object> parser = testUtils.getTestObjectMapper().convertValue(
        new StringInputRowParser(parseSpec, Charset.defaultCharset().name()),
        Map.class
    );
    return new DataSchema(
        "test",
        parser,
        aggregators,
        granularitySpec,
        testUtils.getTestObjectMapper()
    );
  }

  @Test
  public void basicLoadTest() throws Exception
  {
    final List<String> dimensions = Arrays.asList(
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment"
    );

    final TimestampSpec timestampSpec = new TimestampSpec("l_orderkey", null, null)
    {
      private int indexer = 0;

      @Override
      public DateTime extractTimestamp(Map<String, Object> input)
      {
        return new DateTime(indexer++);
      }
    };
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensions, null, null);
    final ParseSpec parseSpec = new DelimitedParseSpec(timestampSpec, dimensionsSpec, "|", null, dimensions);

    final FirehoseFactory factory = new LocalFirehoseFactory(
        new File("/Users/navis/tpch_2_17_0/data"),
        "*.tbl",
        null
    );

    final DataSchema schema = toDataSchema(parseSpec, new AggregatorFactory[0], null);

    final RealtimeTuningConfig tuning =
        RealtimeTuningConfig.makeDefaultTuningConfig()
                            .withV9()
                            .withMaxPendingPersists(-1);

    runTest(schema, factory, tuning);
  }

  @Test
  public void dfnLoadTest() throws Exception
  {
    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto", null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null, null, null);
    final ParseSpec parseSpec = new JSONParseSpec(timestampSpec, dimensionsSpec);

    final FirehoseFactory factory = new LocalFirehoseFactory(
        new File("/Users/navis/tmp/hynix"),
        "summary_DFN409_all.json",
        null
    );

    final DataSchema schema = toDataSchema(
        parseSpec,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        new UniformGranularitySpec(
            Granularity.HOUR,
            QueryGranularity.NONE,
            Arrays.asList(Interval.parse("2015-11-10/2015-12-30"))
        )
    );

    final RealtimeTuningConfig tuning = RealtimeTuningConfig.makeDefaultTuningConfig().withV9().withMaxRow(200000);

    runTest(schema, factory, tuning);
  }

  private void runTest(DataSchema schema, FirehoseFactory factory, RealtimeTuningConfig tuning) throws Exception
  {
    final HeapMemoryTaskStorage taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    final TaskLockbox taskLockbox = new TaskLockbox(taskStorage);
    final TaskActionToolbox toolbox = new TaskActionToolbox(
        taskLockbox,
        new TestIndexerMetadataStorageCoordinator(),
        new NoopServiceEmitter()
    );
    final TaskActionClientFactory taskActionClientFactory = new TaskActionClientFactory()
    {
      @Override
      public TaskActionClient create(Task task)
      {
        return new LocalTaskActionClient(task, taskStorage, toolbox);
      }
    };
    final String baseDir = Files.createTempDir().toString();
    final TaskConfig taskConfig = new TaskConfig(baseDir, null, null, 0, null, false, null, null);

    final SegmentHandoffNotifierFactory handoffNotifierFactory = new SegmentHandoffNotifierFactory()
    {
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
            return true;
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
    };

    final TaskToolboxFactory toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        taskActionClientFactory,
        new NoopServiceEmitter(),
        new TestDataSegmentPusher(),
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        handoffNotifierFactory,
        new DefaultQueryRunnerFactoryConglomerate(Maps.<Class<? extends Query>, QueryRunnerFactory>newHashMap()),
        MoreExecutors.sameThreadExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }, testUtils.getTestObjectMapper()
            )
        ),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexMerger(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        testUtils.getTestIndexMergerV9()
    );

    final FireDepartment department = new FireDepartment(
        schema,
        new RealtimeIOConfig(factory, null, null),
        tuning
    );

    RealtimeIndexTask task = new RealtimeIndexTask("test", new TaskResource("", 0), department, null);
    taskLockbox.add(task);

    long start = System.currentTimeMillis();
    TaskStatus status = task.run(toolboxFactory.build(task));

    System.out.println(status + " in: " + baseDir + ", took: " + (System.currentTimeMillis() - start) + " msec");
  }

  @Test
  public void test() throws IOException
  {
    File loc = new File("/Users/navis/temporary/1970-01-01T00:00:00.000Z_1970-01-02T00:00:00.000Z");

    IndexIO indexIO = testUtils.getTestIndexIO();

    IndexableAdapter[] adapters = new QueryableIndexIndexableAdapter[12];
    for (int i = 0; i < adapters.length; i++) {
      adapters[i] = new QueryableIndexIndexableAdapter(indexIO.loadIndex(new File(loc, String.valueOf(i))));
    }

    File out = new File(loc, "test-merge");
    RealtimeTuningConfig config = RealtimeTuningConfig.makeDefaultTuningConfig();
    testUtils.getTestIndexMerger()
             .merge(
                 Arrays.asList(adapters),
                 new AggregatorFactory[0],
                 out,
                 config.getIndexSpec()
             );
  }
}
