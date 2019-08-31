/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.tests.cooperativesource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.GenericPredicates;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.projection.Projections;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import javax.cache.Cache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CooperativeMapCacheSourceTest extends AbstractSoakTest {

    private static final String TEST_NAME = CooperativeMapCacheSourceTest.class.getSimpleName();
    private static final String SOURCE_MAP = TEST_NAME + "_SourceMap";
    private static final String SINK_LOCAL_MAP = TEST_NAME + "_SinkLocalMap";
    private static final String SINK_REMOTE_MAP = TEST_NAME + "_SinkRemoteMap";
    private static final String SINK_LOCAL_CACHE = TEST_NAME + "_SinkLocalCache";
    private static final String SINK_REMOTE_CACHE = TEST_NAME + "_SinkRemoteCache";
    private static final String SINK_QUERY_LOCAL_MAP = TEST_NAME + "_SinkQueryLocalMap";
    private static final String SINK_QUERY_REMOTE_MAP = TEST_NAME + "_SinkQueryRemoteMap";
    private static final String SOURCE_CACHE = TEST_NAME + "_SourceCache";
    private static final int SOURCE_MAP_ITEMS = 1_000;
    private static final int SOURCE_MAP_LAST_KEY = SOURCE_MAP_ITEMS - 1;
    private static final int SOURCE_CACHE_ITEMS = 1_000;
    private static final int SOURCE_CACHE_LAST_KEY = SOURCE_CACHE_ITEMS - 1;
    private static final int PREDICATE_FROM = 500;
    private static final int EXPECTED_SIZE_AFTER_PREDICATE = SOURCE_MAP_ITEMS - PREDICATE_FROM;

    private static final int LOCAL_MAP_TEST_INDEX = 0;
    private static final int REMOTE_MAP_TEST_INDEX = 1;
    private static final int QUERY_LOCAL_MAP_TEST_INDEX = 2;
    private static final int QUERY_REMOTE_MAP_TEST_INDEX = 3;
    private static final int LOCAL_CACHE_TEST_INDEX = 4;
    private static final int REMOTE_CACHE_TEST_INDEX = 5;
    private static final int NUMBER_OF_TESTS = 6;

    private static final int DEFAULT_THREAD_COUNT = 1;

    private int threadCount;
    /**
     * I'm deliberately use only one instance of Exception instead of something like Exception per source type. <br/>
     * We will stop all tests once when any Exception occurs.
     */
//    private volatile Exception exception;

    private final ConcurrentHashMap<Integer, Exception> exceptions = new ConcurrentHashMap<>();
    private int[] sequence;

//    private int[] localMapSequence;
//    private int[] remoteMapSequence;
//    private int[] queryLocalMapSequence;
//    private int[] queryRemoteMapSequence;
//    private int[] localCacheSequence;
//    private int[] remoteCacheSequence;

    public static void main(String[] args) throws Exception {
        new CooperativeMapCacheSourceTest().run(args);
    }

    @Override
    public void init() {
        threadCount = propertyInt("cooperative_map_cache_thread_count", DEFAULT_THREAD_COUNT);
//        localMapSequence = new int[threadCount];
//        remoteMapSequence = new int[threadCount];
//        queryLocalMapSequence = new int[threadCount];
//        queryRemoteMapSequence = new int[threadCount];
//        localCacheSequence = new int[threadCount];
//        remoteCacheSequence = new int[threadCount];
        sequence = new int[NUMBER_OF_TESTS * threadCount];

        initializeSourceMap();
        initializeSourceCache();
    }
    @Override
    public void test() throws Exception {
        List<ExecutorService> executorServices = new ArrayList<>();
        executorServices.add(runTestInExecutorService(
                index -> executeLocalMapJob(index),
                index -> verifyLocalMapJob(index),
                LOCAL_MAP_TEST_INDEX
        ));
        executorServices.add(runTestInExecutorService(
                index -> executeRemoteMapJob(index),
                index -> verifyRemoteMapJob(index),
                REMOTE_MAP_TEST_INDEX
        ));
        executorServices.add(runTestInExecutorService(
                index -> executeQueryLocalMapJob(index),
                index -> verifyQueryLocalMapJob(index),
                QUERY_LOCAL_MAP_TEST_INDEX
        ));
        executorServices.add(runTestInExecutorService(
                index -> executeQueryRemoteMapJob(index),
                index -> verifyQueryRemoteMapJob(index),
                QUERY_REMOTE_MAP_TEST_INDEX
        ));
        executorServices.add(runTestInExecutorService(
                index -> executeLocalCacheJob(index),
                index -> verifyLocalCacheJob(index),
                LOCAL_CACHE_TEST_INDEX
        ));
        executorServices.add(runTestInExecutorService(
                index -> executeRemoteCacheJob(index),
                index -> verifyRemoteCacheJob(index),
                REMOTE_CACHE_TEST_INDEX
        ));

        awaitExecutorServiceTermination(executorServices);

        evaluateExceptions();
//        if (exception != null) {
//            throw exception;
//        }
    }

    @Override
    public void teardown() {
    }

    private ExecutorService runTestInExecutorService(Consumer<Integer> executeJob, Consumer<Integer> verify,
            int testIndex) {
        long begin = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                while ((System.currentTimeMillis() - begin) < durationInMillis
                        && !exceptions.contains(testIndex)) {
                    //exception == null) {
                    try {
                        int index = threadIndex * NUMBER_OF_TESTS + testIndex;
                        executeJob.accept(threadIndex);
                        verify.accept(threadIndex);
                        sequence[index]++;
                    } catch (Throwable e) {
                        exceptions.putIfAbsent(testIndex, new Exception(e));
//                        exception = new Exception(e);
                    }
                }
            });
        }
        executorService.shutdown();
        return executorService;
    }

    private void executeLocalMapJob(int index) {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.map(SOURCE_MAP))
                .map((t) -> {
            t.setValue(t.getValue() + "_" + sequence[index]);
                    return t;
                })
                .drainTo(Sinks.map(SINK_LOCAL_MAP + index));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Local Map [thread " + (index / NUMBER_OF_TESTS) + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeRemoteMapJob(int index) {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.remoteMap(SOURCE_MAP, wrappedRemoteClusterClientConfig()))
                .map((t) -> {
            t.setValue(t.getValue() + "_" + sequence[index]);
                    return t;
                })
                .drainTo(Sinks.map(SINK_REMOTE_MAP + index));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Remote Map [thread " + (index / NUMBER_OF_TESTS) + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeQueryLocalMapJob(int index) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source
                = Sources.<Map.Entry<Integer, String>, Integer, String>map(
                        SOURCE_MAP,
                        GenericPredicates.greaterEqual("__key", PREDICATE_FROM),
                        Projections.identity());
        pipeline.drawFrom(source)
                .map((t) -> {
            t.setValue(t.getValue() + "_" + sequence[index]);
                    return t;
                })
                .drainTo(Sinks.map(SINK_QUERY_LOCAL_MAP + index));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Query Local Map [thread " + (index / NUMBER_OF_TESTS) + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeQueryRemoteMapJob(int index) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source
                = Sources.<Map.Entry<Integer, String>, Integer, String>remoteMap(
                        SOURCE_MAP,
                        wrappedRemoteClusterClientConfig(),
                        GenericPredicates.greaterEqual("__key", PREDICATE_FROM),
                        Projections.identity());
        pipeline.drawFrom(source)
                .map((t) -> {
            t.setValue(t.getValue() + "_" + sequence[index]);
                    return t;
                })
                .drainTo(Sinks.map(SINK_QUERY_REMOTE_MAP + index));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Query Remote Map [thread " + (index / NUMBER_OF_TESTS) + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeLocalCacheJob(int index) {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.cache(SOURCE_CACHE))
                .map((t) -> {
            t.setValue(t.getValue() + "_" + sequence[index]);
                    return t;
                })
                .drainTo(Sinks.map(SINK_LOCAL_CACHE + index));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Local Cache [thread " + (index / NUMBER_OF_TESTS) + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeRemoteCacheJob(int index) {
        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(Sources.remoteCache(SOURCE_CACHE, wrappedRemoteClusterClientConfig()))
                .map((t) -> {
            t.setValue(t.getValue() + "_" + sequence[index]);
                    return t;
                })
                .drainTo(Sinks.map(SINK_REMOTE_CACHE + index));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Remote Cache [thread " + (index / NUMBER_OF_TESTS) + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void verifyLocalMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_LOCAL_MAP + threadIndex);
        assertEquals(SOURCE_MAP_ITEMS, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + sequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyRemoteMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_REMOTE_MAP + threadIndex);
        assertEquals(SOURCE_MAP_ITEMS, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + sequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyQueryLocalMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_QUERY_LOCAL_MAP + threadIndex);
        assertEquals(EXPECTED_SIZE_AFTER_PREDICATE, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + sequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyQueryRemoteMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_QUERY_REMOTE_MAP + threadIndex);
        assertEquals(EXPECTED_SIZE_AFTER_PREDICATE, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + sequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyLocalCacheJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_LOCAL_CACHE + threadIndex);
        assertEquals(SOURCE_CACHE_ITEMS, map.size());
        String value = map.get(SOURCE_CACHE_LAST_KEY);
        String expectedValue = SOURCE_CACHE_LAST_KEY + "_" + sequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyRemoteCacheJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_REMOTE_CACHE + threadIndex);
        assertEquals(SOURCE_CACHE_ITEMS, map.size());
        String value = map.get(SOURCE_CACHE_LAST_KEY);
        String expectedValue = SOURCE_CACHE_LAST_KEY + "_" + sequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void awaitExecutorServiceTermination(List<ExecutorService> executorServices) throws InterruptedException {
        for (ExecutorService executorService : executorServices) {
            executorService.awaitTermination(durationInMillis + MINUTES.toMillis(1), MILLISECONDS);
        }
    }

    private void evaluateExceptions() throws Exception {
        if (!exceptions.isEmpty()) {
            logger.info("There were " + exceptions.size() + " failed tests in " + TEST_NAME);
            Set<Map.Entry<Integer, Exception>> entrySet = exceptions.entrySet();
            exceptions.entrySet().forEach((entry) -> {
                entry.getValue().printStackTrace();
            });
            throw entrySet.iterator().next().getValue();
        }
    }

    private void initializeSourceMap() {
        Map<Integer, String> map = jet.getMap(SOURCE_MAP);
        for (int i = 0; i < SOURCE_MAP_ITEMS; i++) {
            map.put(i, Integer.toString(i));
        }
    }

    private ClientConfig wrappedRemoteClusterClientConfig() {
        try {
            return remoteClusterClientConfig();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void initializeSourceCache() {
        Cache<Integer, String> cache = jet.getCacheManager().getCache(SOURCE_CACHE);
        for (int i = 0; i < SOURCE_MAP_ITEMS; i++) {
            cache.put(i, Integer.toString(i));
        }
    }

}
