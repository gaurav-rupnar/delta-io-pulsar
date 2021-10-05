package org.apache.pulsar.io.delta.source;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DeltaSnapshotListingThread extends Thread {

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
    private final Lock listingLock = new ReentrantLock();
    private final BlockingDeque<Long> snapshotToProcess;
    private final BlockingDeque<Long> snapshotProcessing;
    private final BlockingDeque<Long> snapshotProcessed;

    private final String deltaTableLocation;
    private final long pollingInterval;

    public DeltaSnapshotListingThread(DeltaSourceConfig deltaSourceConfig,
                                      BlockingDeque<Long> snapshotToProcess,
                                      BlockingDeque<Long> snapshotProcessing,
                                      BlockingDeque<Long> snapshotProcessed){

        this.snapshotProcessed = snapshotProcessed;
        this.snapshotProcessing = snapshotProcessing;
        this.snapshotToProcess = snapshotToProcess;

        this.deltaTableLocation = deltaSourceConfig.getDeltaTablePath();
        this.pollingInterval = deltaSourceConfig.getPollingInterval();
    }

    public void run(){
        while (true){
            if((queueLastUpdated.get() < System.currentTimeMillis()-pollingInterval) && listingLock.tryLock()){
                try{
                    // load the delta table
                    // get the current snapshot number
                    //
                }finally {
                    listingLock.unlock();
                }
            }
            try {
                sleep(pollingInterval - 1);
            } catch (InterruptedException e) {
                // Just ignore
            }
        }
    }
}
