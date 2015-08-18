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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import kafka.message.MessageAndMetadata;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.assistedinject.Assisted;
import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.adaptor.avro.AvroAdaptorException;
import com.parstream.driver.ColumnInfo;

/**
 * Kafka message decoder for Avro payload.
 */
public class AvroMessageDecoder implements MessageDecoder {

    private static final Log LOG = LogFactory.getLog(AvroMessageDecoder.class);

    private AvroAdaptor _adaptor;
    private GenericRecord _record;

    @Inject
    AvroMessageDecoder(@Named("mapping file") File mappingIni, @Assisted ColumnInfo[] columnInfo)
            throws AvroAdaptorException, IOException {
        _adaptor = new AvroAdaptor(mappingIni, columnInfo);
    }

    @Override
    public List<Object[]> decodeMessage(MessageAndMetadata<byte[], byte[]> kafkaMsg) {
        List<Object[]> result = new ArrayList<Object[]>(0);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();

        try (DataFileReader<GenericRecord> messageReader = new DataFileReader<GenericRecord>(
                new SeekableByteArrayInput(kafkaMsg.message()), datumReader)) {
            while (messageReader.hasNext()) {
                _record = messageReader.next(_record);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("received record: " + _record.toString());
                }

                try {
                    result.addAll(_adaptor.convertRecord(_record));
                } catch (AvroAdaptorException e) {
                    LOG.error("undecodable message", e);
                }
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("unparsable schema", e);
        } catch (IOException e) {
            LOG.error("unreadable avro record", e);
        }
        return result;
    }
}
