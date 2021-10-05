package org.apache.pulsar.io.delta.source;

import lombok.Data;
import org.apache.pulsar.functions.api.Record;
@Data
public class DeltaRecord implements Record<byte []> {

    private final byte[] value;

    public DeltaRecord(byte[] value) {
        this.value = value;
    }


    @Override
    public byte[] getValue() {
        return value;
    }
}
