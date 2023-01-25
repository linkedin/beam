/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.samza.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Map;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.samza.config.Config;

public class SamzaOpUtils {
  public static Map<String, Map.Entry<String, String>> getTransformToIOMap(Config config) {
    TypeReference<Map<String, Map.Entry<String, String>>> typeRef =
        new TypeReference<Map<String, Map.Entry<String, String>>>() {};
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(
          new SimpleModule()
              .addDeserializer(
                  Map.Entry.class, new SamzaPipelineTranslator.MapEntryDeserializer()));
      // read the BeamTransformIoMap from config deserialize
      return objectMapper.readValue(config.get(SamzaRunner.BEAM_TRANSFORMS_WITH_IO), typeRef);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format(
              "Cannot deserialize %s from the configs", SamzaRunner.BEAM_TRANSFORMS_WITH_IO),
          e);
    }
  }
}
