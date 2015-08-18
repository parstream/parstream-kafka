/**
 * Copyright 2015 ParStream GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.parstream.adaptor.kafka;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;

import kafka.message.MessageAndMetadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.assistedinject.Assisted;
import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.JsonAdaptorException;
import com.parstream.driver.ColumnInfo;

/**
 * Kafka message decoder for JSON payload.
 */
public class JsonMessageDecoder implements MessageDecoder {

    private static final Log LOG = LogFactory.getLog(JsonMessageDecoder.class);

    private JsonAdaptor _adaptor;

    @Inject
    JsonMessageDecoder(@Named("mapping file") File mappingIni, @Assisted ColumnInfo[] columnInfo)
            throws JsonAdaptorException, IOException {
        _adaptor = new JsonAdaptor(mappingIni, columnInfo);
    }

    @Override
    public List<Object[]> decodeMessage(MessageAndMetadata<byte[], byte[]> kafkaMsg) {
        List<Object[]> result = new ArrayList<Object[]>(0);
        if (kafkaMsg != null) {
            String decodedEvent = "";
            try {
                decodedEvent = new String(kafkaMsg.message(), "UTF-8");
                JsonReader jsonReader = Json.createReader(new StringReader(decodedEvent));
                JsonObject jsonObj = jsonReader.readObject();
                result = _adaptor.convertJson(jsonObj);
            } catch (UnsupportedEncodingException e) {
                LOG.error("unparsable message", e);
            } catch (JsonAdaptorException e) {
                LOG.error("undecodable message", e);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("message = " + decodedEvent);
                }
            } catch (JsonParsingException e) {
                LOG.error("invalid JSON", e);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("message = " + decodedEvent);
                }
            }
        }

        return result;
    }
}
