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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SamzaOpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOpUtils.class);
  public static final String TRANSFORM_IO_MAP_DELIMITER = ",";

  public static Map<String, Map.Entry<String, String>> deserializeTransformIOMap(Config config) {
    TypeReference<Map<String, Map.Entry<String, String>>> typeRef =
        new TypeReference<Map<String, Map.Entry<String, String>>>() {};
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(
          new SimpleModule()
              .addDeserializer(
                  Map.Entry.class, new MapEntryDeserializer()));
      return objectMapper.readValue(config.get(SamzaRunner.BEAM_TRANSFORMS_WITH_IO), typeRef);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Cannot deserialize %s from the configs", SamzaRunner.BEAM_TRANSFORMS_WITH_IO), e);
    }
  }

  public static String serializeTransformIOMap(Map<String, Map.Entry<String, String>> pTransformToIOMap) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(
          new SimpleModule().addSerializer(Map.Entry.class, new MapEntrySerializer()));
      return objectMapper.writeValueAsString(pTransformToIOMap);
    } catch (IOException e) {
      LOG.error(
          "Unable to serialize {} using {}",
          SamzaRunner.BEAM_TRANSFORMS_WITH_IO,
          pTransformToIOMap);
      e.printStackTrace();
    }
    return null;
  }

  @SuppressWarnings({"rawtypes"})
  public static final class MapEntrySerializer extends JsonSerializer<Map.Entry> {
    @Override
    public void serialize(Map.Entry value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeObjectField("left", value.getKey());
      gen.writeObjectField("right", value.getValue());
      gen.writeEndObject();
    }
  }


  @SuppressWarnings({"rawtypes"})
  public static final class MapEntryDeserializer extends JsonDeserializer<Map.Entry> {
    @Override
    public Map.Entry deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);
      String key = node.get("left").textValue();
      String value = node.get("right").textValue();
      return new AbstractMap.SimpleEntry<>(key, value);
    }
  }
}
