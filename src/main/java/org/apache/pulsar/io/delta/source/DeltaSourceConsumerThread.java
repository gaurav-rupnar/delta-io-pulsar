package org.apache.pulsar.io.delta.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.delta.source.utils.RowConversion;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DeltaSourceConsumerThread extends Thread{
    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
    private final Lock listingLock = new ReentrantLock();
    private final PushSource<byte[]> source;
    private final String deltaTableLocation;
    private final long pollingInterval;
    private final DeltaTableReader deltaTableReader = new DeltaTableReader();
    private Long previousSnapshotVersion;

    public DeltaSourceConsumerThread(PushSource<byte[]> source, DeltaSourceConfig deltaSourceConfig,Long previousSnapshotVersion) {
        this.source = source;
        this.deltaTableLocation = deltaSourceConfig.getDeltaTablePath();
        this.pollingInterval = deltaSourceConfig.getPollingInterval();
        this.previousSnapshotVersion = previousSnapshotVersion;
    }

    private Long getPreviousVersion(){

        return previousSnapshotVersion;
    }
    public void run() {

        // load the delta table
        // get current version
        // check previous version memory {we need to checkpoint this to disk}
        //
        DeltaLog deltaLog = deltaTableReader.loadDeltaTable(deltaTableLocation);
        while (true){
            if((queueLastUpdated.get() < System.currentTimeMillis()-pollingInterval) && listingLock.tryLock()){

                Long version = deltaLog.update().getVersion();
                Long previousVersion = getPreviousVersion();
                if(previousVersion!=0l){
                    for (Long snapshot =previousVersion+1 ; snapshot<=version ; snapshot++ ){

                        try {
                            readRecords(deltaTableReader.loadSnapshot(deltaLog, snapshot));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                }
                else {
                    try {
                        readRecords(deltaTableReader.loadSnapshot(deltaLog, version));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }


            }
            try {
                sleep(pollingInterval -1);
            }
            catch (InterruptedException e)
            {
                System.out.println("Interrupted");
            }
        }

    }

    private void readRecords(CloseableIterator<RowRecord> loadSnapshot) throws JsonProcessingException {
        System.out.println("\ndata rows:");
        RowRecord row = null;
        int numRows = 0;
        while (loadSnapshot.hasNext()) {
            row = loadSnapshot.next();

            String rowJson = RowConversion.rowRecordToJson(row);
            source.consume(new DeltaRecord(rowJson.getBytes()));

            numRows++;

            Long c1 = row.isNullAt("c1") ? null : row.getLong("c1");
            Long c2 = row.isNullAt("c2") ? null : row.getLong("c2");
            String c3 = row.getString("c3");
            System.out.println(c1 + " " + c2 + " " + c3);
        }
    }

}
