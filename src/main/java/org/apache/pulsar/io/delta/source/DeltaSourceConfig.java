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

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
@Data
@Accessors(chain = true)
public class DeltaSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String deltaTablePath;

    private Long pollingInterval;

    private String checkpointDirectory;

    public static DeltaSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), DeltaSourceConfig.class);
    }

    public static DeltaSourceConfig load(Map<String,Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map),DeltaSourceConfig.class);
    }

    public void setDeltaTablePath(String deltaTablePath) {
        this.deltaTablePath = deltaTablePath;
    }

    public void setPollingInterval(Long pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public void setCheckpointDirectory(String checkpointDirectory) {
        this.checkpointDirectory = checkpointDirectory;
    }
}
