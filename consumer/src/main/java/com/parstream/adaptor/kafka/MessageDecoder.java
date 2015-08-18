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

import java.util.List;

import kafka.message.MessageAndMetadata;

/**
 * Interface for decoding Kafka message.
 */
public interface MessageDecoder {

    /**
     * Convert a single Kafka message into ParStream table rows.
     * <p>
     * Note: A single message can be converted into multiple table rows.
     *
     * @param message
     *            Kafka message
     * @return ParStream table rows
     */
    public List<Object[]> decodeMessage(MessageAndMetadata<byte[], byte[]> message);
}
