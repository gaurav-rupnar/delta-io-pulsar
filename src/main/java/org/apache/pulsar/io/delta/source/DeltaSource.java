/**
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

package org.apache.pulsar.io.delta.source;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;
import java.util.concurrent.*;

/*
* check if it's delta table
*
*
* */
public class DeltaSource extends PushSource<byte[]> {

    private ExecutorService executor;

    private final BlockingDeque<Long> snapshotToProcess = new LinkedBlockingDeque<>();
    private final BlockingDeque<Long> snapshotProcessing = new LinkedBlockingDeque<>();
    private final BlockingDeque<Long> snapshotProcessed = new LinkedBlockingDeque<>();

    private Long previousSnapshotVersion = null;
    @Override
    public void open(Map config, SourceContext sourceContext) throws Exception {

        DeltaSourceConfig deltaSourceConfig = DeltaSourceConfig.load(config);
        previousSnapshotVersion = 0l; // to do: load from checkpoint directory
        executor = Executors.newFixedThreadPool(1);
        executor.execute(new DeltaSourceConsumerThread(this,deltaSourceConfig,previousSnapshotVersion));
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
