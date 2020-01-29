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

package com.hazelcast.jet.tests.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.map.IMap;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class JobManagementTest extends AbstractSoakTest {

    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int EVENT_JOURNAL_CAPACITY = 1_500_000;
    private static final int MAP_CLEAR_THRESHOLD = 5000;
    private static final int PRODUCER_SLEEP_MILLIS = 5;

    private static final String SOURCE = JobManagementTest.class.getSimpleName();

    private Producer producer;
    private int snapshotIntervalMs;

    private transient boolean odds;

    public static void main(String[] args) throws Exception {
        new JobManagementTest().run(args);
    }

    public void init() {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        Config config = jet.getHazelcastInstance().getConfig();
        MapConfig mapConfig = new MapConfig(SOURCE);
        mapConfig.getEventJournalConfig()
                .setCapacity(EVENT_JOURNAL_CAPACITY)
                .setEnabled(true);
        config.addMapConfig(mapConfig);
        producer = new Producer(jet.getMap(SOURCE));
        producer.start();
    }

    public void test() {
        // Submit the job without initial snapshot
        Job job = jet.newJob(pipeline(odds), jobConfig(null));
        waitForJobStatus(job, RUNNING);
        sleepMinutes(1);


        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            logger.info("Suspend the job");
            job.suspend();
            waitForJobStatus(job, SUSPENDED);
            sleepMinutes(1);

            logger.info("Resume the job");
            job.resume();
            waitForJobStatus(job, RUNNING);
            sleepMinutes(1);

            logger.info("Restart the job");
            job.restart();
            waitForJobStatus(job, RUNNING);
            sleepMinutes(1);

            logger.info("Cancel job and export snapshot");
            JobStateSnapshot exportedSnapshot = job.cancelAndExportSnapshot("JobManagementTestSnapshot");
            sleepMinutes(1);

            // Change the job (filters either odds or evens)
            odds = !odds;

            logger.info("Upgrade job");
            job = jet.newJob(pipeline(odds), jobConfig(exportedSnapshot.name()));
            waitForJobStatus(job, RUNNING);
            sleepMinutes(1);

            // Destroy the initial snapshot, by now job should produce new
            // snapshots for restart so it is safe.
            exportedSnapshot.destroy();
        }

        job.cancel();
    }

    protected void teardown(Throwable t) throws Exception {
        if (producer != null) {
            producer.stop();
        }
    }

    private JobConfig jobConfig(String initialSnapshot) {
        return new JobConfig()
                .setName("JobManagementTest")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setInitialSnapshotName(initialSnapshot)
                .setAutoScaling(false);
    }

    private static Pipeline pipeline(boolean odds) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(SOURCE, START_FROM_OLDEST, mapEventNewValue(), filter(odds)))
         .withoutTimestamps()
         .groupingKey(l -> 0L)
                .mapUsingService(sharedService(ctw -> null), (c, k, v) -> v)
                .writeTo(Sinks.fromProcessor("sink", VerificationProcessor.supplier(odds)));
        return p;
    }

    static class Producer {

        private final IMap<Long, Long> map;
        private final Thread thread;

        private volatile boolean producing = true;

        Producer(IMap<Long, Long> map) {
            this.map = map;
            this.thread = new Thread(this::run);
        }

        void run() {
            long counter = 0;
            while (producing) {
                try {
                    map.set(counter, counter);
                } catch (Exception e) {
                    e.printStackTrace();
                    sleepSeconds(1);
                    continue;
                }
                counter++;
                if (counter % MAP_CLEAR_THRESHOLD == 0) {
                    map.clear();
                }
                sleepMillis(PRODUCER_SLEEP_MILLIS);
            }
        }

        void start() {
            thread.start();
        }

        void stop() throws InterruptedException {
            producing = false;
            thread.join();
        }
    }

}
