package org.apache.pulsar.io.delta.source;

import org.apache.pulsar.io.core.SourceContext;

import java.util.Map;

public class TestApp {
    public static void main(String[] args) throws Exception {



        DeltaSourceConfig ds = new DeltaSourceConfig();

        ds.setDeltaTablePath("/Users/gauravsuresh.rupnar/git_code/test_data/pulsar/delta_standalone_test2/");
        ds.setPollingInterval(5000l);
        ds.setCheckpointDirectory("/tmp/tmp_90/");

        DeltaSource deltaSource = new DeltaSource();
        Map config = null;
        SourceContext soruceContext = null;
        deltaSource.open(config,soruceContext);

    }
}
