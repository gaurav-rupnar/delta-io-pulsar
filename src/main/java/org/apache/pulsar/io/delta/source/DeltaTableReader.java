package org.apache.pulsar.io.delta.source;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import org.apache.hadoop.conf.Configuration;


import java.util.Objects;

public class DeltaTableReader {


    public  DeltaLog loadDeltaTable(String tableLocation) {

        DeltaLog deltaLog = DeltaLog.forTable(new Configuration(),tableLocation);

        return deltaLog;
    }

    public CloseableIterator<RowRecord> loadSnapshot(DeltaLog deltaLog,Long snapshotVersion) {

        Snapshot snapshot = null;
        CloseableIterator<RowRecord> recordIterator = null;
        if(!Objects.isNull(snapshotVersion)){
            snapshot = deltaLog.getSnapshotForVersionAsOf(snapshotVersion);
            recordIterator = snapshot.open();
        }
        else {
            throw new IllegalArgumentException("Snapshot version is null");
        }
        return recordIterator;
    }

    public CloseableIterator<RowRecord> loadSnapshot(DeltaLog deltaLog) {

        CloseableIterator<RowRecord> recordIterator = null;
        if(!Objects.isNull(deltaLog)){
            recordIterator = deltaLog.snapshot().open();
        }
        else {
            throw new IllegalArgumentException("invalid delta table");
        }
        return recordIterator;
    }




}
