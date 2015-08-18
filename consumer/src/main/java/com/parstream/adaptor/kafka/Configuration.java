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
import java.util.Properties;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Parses the ParStream configuration file, ensure all required configuration
 * keys are defined, and that provided values match expected data type.
 */
public class Configuration {
    private static final Log LOG = LogFactory.getLog(Configuration.class);

    private static final String TOPIC = "topic";
    private static final String TABLE_NAME = "parstream.import.table";
    private static final String DECODER_CLASS = "parstream.decoder.type";
    private static final String DECODER_MAPPINGFILE = "parstream.decoder.mappingfile";

    private static final String COMMIT_ORDER = "parstream.commit.order";
    private static final String COMMIT_ROWCOUNT = "parstream.autocommit.rowcount";
    private static final String COMMIT_TIMESPAN = "parstream.autocommit.timespan";
    private static final String KAFKA_AUTO_COMMIT = "auto.commit.enable";

    private static final String DB_HOST = "parstream.server.address";
    private static final String DB_PORT = "parstream.server.port";
    private static final String DB_TIMEOUT = "parstream.server.timeout";
    private static final String DB_USERNAME = "parstream.server.username";
    private static final String DB_PASSWORD = "parstream.server.password";

    private Long _commitRowCount;
    private Long _commitTimeSpan;

    /** Options for the commit order used by the application. */
    public enum CommitOrder {
        /** Commit first ParStream then Kafka. */
        PARSTREAM_CONSUMER,
        /** Commit first Kafka then ParStream. */
        CONSUMER_PARSTREAM
    };

    private CommitOrder _commitOrder;

    private Class<? extends MessageDecoder> _decoderClass;
    private int _databaseTimeout;

    private Properties _properties;
    private Properties _consumerProperties;

    /**
     * Instantiates a new application configuration from the given database and
     * consumer properties.
     * 
     * @param properties
     *            database properties
     * @param consumerProperties
     *            consumer properties
     */
    public Configuration(Properties properties, Properties consumerProperties) {
        _properties = properties;
        _consumerProperties = consumerProperties;
        validateParstreamProperties();
        validateConsumerProperties();

        // we need to disable Kafka's auto-commit
        if (!_consumerProperties.containsKey(KAFKA_AUTO_COMMIT)
                || Boolean.valueOf(_consumerProperties.getProperty(KAFKA_AUTO_COMMIT))) {
            LOG.warn("ignoring property auto.commit.enable");
            _consumerProperties.setProperty(KAFKA_AUTO_COMMIT, "false");
        }
    }

    private void validateParstreamProperties() throws IllegalArgumentException, NullPointerException {
        Validate.notNull(_properties.getProperty(DB_HOST),
                "No Parstream server host specified. Use configuration key: " + DB_HOST);

        validatePort();

        Validate.notNull(_properties.getProperty(DECODER_CLASS),
                "No Parstream decoder type specified. Use configuration key: " + DECODER_CLASS);

        validateDecoderClass();

        Validate.notNull(_properties.getProperty(DECODER_MAPPINGFILE),
                "No Parstream decoder mapping file specified. Use configuration key: " + DECODER_MAPPINGFILE);

        Validate.notNull(_properties.getProperty(TABLE_NAME),
                "No Parstream table name specified. Use configuration key: " + TABLE_NAME);

        validateCommitStrategy();
        validateCommitOrder();
        validateConnectionTimeout();
    }

    private void validateConsumerProperties() {
        Validate.notNull(_consumerProperties.getProperty(TOPIC),
                "must specify topic name in consumer properties file. Use configuration key: " + TOPIC);
    }

    private void validatePort() {
        Validate.notNull(_properties.getProperty(DB_PORT),
                "No Parstream server port specified. Use configuration key: " + DB_PORT);
    }

    @SuppressWarnings("unchecked")
    private void validateDecoderClass() {
        String decoder = _properties.getProperty(DECODER_CLASS);
        Validate.notNull(decoder, "No Parstream decoder specified. Use configuration key: " + DECODER_CLASS);

        // try to load the decoder class
        try {
            _decoderClass = (Class<? extends MessageDecoder>) Class.forName(_properties.getProperty(DECODER_CLASS));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Invalid Parstream decoder specified.", e);
        }
    }

    private void validateCommitStrategy() {
        String rowCount = _properties.getProperty(COMMIT_ROWCOUNT);
        String timeSpan = _properties.getProperty(COMMIT_TIMESPAN);
        boolean hasCommitStrategy = rowCount != null || timeSpan != null;

        Validate.isTrue(hasCommitStrategy, "No Parstream commit strategy defined. Use either configuration key: "
                + COMMIT_ROWCOUNT + ", or configuration key: " + COMMIT_TIMESPAN);

        if (rowCount != null) {
            try {
                _commitRowCount = Long.parseLong(rowCount);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Specified Parstream commit row count must be a numerical value");
            }
            if (_commitRowCount < 1) {
                throw new IllegalArgumentException("Specified Parstream commit row count must be a positive value");
            }
        }
        if (timeSpan != null) {
            try {
                _commitTimeSpan = Long.parseLong(timeSpan);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Specified Parstream commit time span must be a numerical value");
            }
            if (_commitTimeSpan < 1) {
                throw new IllegalArgumentException("Specified Parstream commit time span must be a positive value");
            }
        }
    }

    private void validateCommitOrder() {
        String selectedValue = _properties.getProperty(COMMIT_ORDER);

        String[] orderSplitted = selectedValue.split(",");
        for (int i = 0; i < orderSplitted.length; ++i) {
            orderSplitted[i] = orderSplitted[i].trim();
        }
        Validate.isTrue(orderSplitted.length == 2, "No valid transaction commit strategy defined.");

        if ("Parstream".equals(orderSplitted[0]) && "Consumer".equals(orderSplitted[1])) {
            _commitOrder = CommitOrder.PARSTREAM_CONSUMER;
        } else if ("Consumer".equals(orderSplitted[0]) && "Parstream".equals(orderSplitted[1])) {
            _commitOrder = CommitOrder.CONSUMER_PARSTREAM;
        } else {
            throw new IllegalArgumentException("No valid transaction commit strategy defined.");
        }
    }

    private void validateConnectionTimeout() {
        String specifiedTimeout = _properties.getProperty(DB_TIMEOUT);
        try {
            _databaseTimeout = Integer.parseInt(specifiedTimeout);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Specified Parstream connection timeout must be a numerical value");
        }
    }

    /**
     * @return database host name
     */
    public String getDatabaseHost() {
        return _properties.getProperty(DB_HOST);
    }

    /**
     * @return database port as string
     */
    public String getDatabasePort() {
        return _properties.getProperty(DB_PORT);
    }

    /**
     * @return auto commit row count, null if not set
     */
    public Long getCommitRateRowCount() {
        return _commitRowCount;
    }

    /**
     * @return auto commit time span, null if not set
     */
    public Long getCommitRateTimeSpan() {
        return _commitTimeSpan;
    }

    /**
     * @return database user account name, null if not set
     */
    public String getDatabaseUsername() {
        return _properties.getProperty(DB_USERNAME);
    }

    /**
     * @return database user's password, null if not set
     */
    public String getDatabasePassword() {
        return _properties.getProperty(DB_PASSWORD);
    }

    /**
     * @return database connect time out, 0 if unlimited
     */
    public int getDatabaseTimeout() {
        return _databaseTimeout;
    }

    /**
     * @return order of issuing commits
     */
    public CommitOrder getCommitOrder() {
        return _commitOrder;
    }

    /**
     * @return table name
     */
    public String getTableName() {
        return _properties.getProperty(TABLE_NAME);
    }

    /**
     * @return class for decoding Kafka messages
     */
    public Class<? extends MessageDecoder> getDecoderClass() {
        return _decoderClass;
    }

    /**
     * @return decoder mapping file
     */
    public File getDecoderMappingFile() {
        // no need to check file, decoder does so
        return new File(_properties.getProperty(DECODER_MAPPINGFILE));
    }

    /**
     * @return Kafka consumer properties
     */
    public Properties getConsumerProperties() {
        return _consumerProperties;
    }

    /**
     * @return consumer topic name
     */
    public String getConsumerTopic() {
        return _consumerProperties.getProperty(TOPIC);
    }
}
