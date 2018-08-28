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

package com.hazelcast.jet.tests.rolling;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.server.JetBootstrap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.rolling.VerificationProcessor;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.function.DistributedComparator.comparing;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnit4.class)
public class RollingAggregateTest {

    private static final String SOURCE = RollingAggregateTest.class.getSimpleName();

    private JetInstance jet;
    private long durationInMillis;
    private long snapshotIntervalMs;

    private volatile boolean producing = true;

    public static void main(String[] args) {
        JUnitCore.main(RollingAggregateTest.class.getName());
    }

    @Before
    public void setUp() {
        System.setProperty("hazelcast.logging.type", "log4j");
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "5")));
        snapshotIntervalMs = Integer.parseInt(System.getProperty("snapshotIntervalMs", "5000"));
        jet = JetBootstrap.getInstance();
        Config config = jet.getHazelcastInstance().getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(SOURCE).setCapacity(500_000)
        );
    }

    @After
    public void teardown() {
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void test() throws Exception {
        Thread thread = new Thread(() -> {
            IMapJet<Long, Long> map = jet.getMap(SOURCE);
            long counter = 0;

            while (producing) {
                map.set(counter, counter++);
                if (counter % 10000 == 0) {
                    map.clear();
                }
            }
        });
        thread.start();

        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.<Long, Long, Long>mapJournal(SOURCE, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
         .rollingAggregate(maxBy(comparing(val -> val)))
         .drainTo(fromProcessor("sink", VerificationProcessor.supplier()));

        JobConfig jobConfig = new JobConfig()
                .addClass(RollingAggregateTest.class)
                .addClass(VerificationProcessor.class)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setProcessingGuarantee(EXACTLY_ONCE);

        Job job = jet.newJob(p, jobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            assertNotEquals(getJobStatus(job), FAILED);
            SECONDS.sleep(5);
        }

        job.cancel();
        producing = false;
        thread.join();

        JobStatus status = getJobStatus(job);
        while (status != COMPLETED && status != FAILED) {
            SECONDS.sleep(1);
            status = getJobStatus(job);
        }
    }

    private static JobStatus getJobStatus(Job job) {
        try {
            return job.getStatus();
        } catch (Exception e) {
            uncheckRun(() -> MILLISECONDS.sleep(250));
            return getJobStatus(job);
        }
    }

}
