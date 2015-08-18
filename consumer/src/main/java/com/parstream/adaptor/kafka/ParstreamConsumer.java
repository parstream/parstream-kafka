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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.inject.Inject;
import javax.inject.Named;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.parstream.driver.ParstreamConnection;
import com.parstream.driver.ParstreamException;
import com.parstream.driver.ParstreamFatalException;

/**
 * This consumer reads event messages from a running Kafka broker, decodes them
 * into a ParStream format and streams them into a running ParStream instance.
 *
 */
public class ParstreamConsumer implements Runnable {

    private static final Log LOG = LogFactory.getLog(ParstreamConsumer.class);
    private static final int STREAM_COUNT_PER_TOPIC = 1;

    private Configuration _config;
    private volatile boolean _shutdown;
    private Object _syncLock = new Object();

    private Properties _consumerProperties;
    private ConsumerConnector _consumer;

    private ParstreamConnection _psConnection;
    private int _pendingRows;

    @Inject
    private DecoderFactory _decoderFactory;
    private Timer _timer;
    private TimerTask _timerTask;

    private enum CommitMode {
        AUTOCOMMIT_ROWCOUNT, AUTOCOMMIT_TIMESPAN, SHUTDOWN_MODE
    };

    /**
     * Kafka consumer for inserting rows into a ParStream table.
     *
     * @param config
     *            configuration for this consumer
     */
    @Inject
    public ParstreamConsumer(@Named("configuration") Configuration config) {
        _config = config;
        _consumerProperties = config.getConsumerProperties();

        if (_config.getCommitRateTimeSpan() != null) {
            _timer = new Timer();
        }
    }

    private void connectParstream() throws ParstreamFatalException, ParstreamException {
        LOG.info("connect to ParStream instance");
        _psConnection = new ParstreamConnection();
        _psConnection.createHandle();
        _psConnection.setTimeout(_config.getDatabaseTimeout());
        _psConnection.connect(_config.getDatabaseHost(), _config.getDatabasePort(), _config.getDatabaseUsername(),
                _config.getDatabasePassword());
        _psConnection.prepareInsert(_config.getTableName());
        _pendingRows = 0;
    }

    private void startCommitTimer() {
        if (_timer != null) {
            _timerTask = new TimerTask() {
                @Override
                public void run() {
                    synchronized (_syncLock) {
                        if (!_shutdown) {
                            try {
                                commit(CommitMode.AUTOCOMMIT_TIMESPAN);
                            } catch (ParstreamFatalException e) {
                                LOG.fatal("unrecoverable ParStream exception encountered", e);
                                _shutdown = true;
                            } catch (ParstreamException e) {
                                // indicates invalid ParStream table name
                                LOG.fatal("unrecoverable ParStream exception encountered", e);
                                _shutdown = true;
                            }
                        }
                    }
                }
            };

            // schedule next auto commit task
            _timer.schedule(_timerTask, _config.getCommitRateTimeSpan() * 1000);
        }
    }

    private KafkaStream<byte[], byte[]> subscribeTopic() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(_config.getConsumerTopic(), STREAM_COUNT_PER_TOPIC);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = _consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(_config.getConsumerTopic());
        if (streams == null || streams.size() == 0) {
            throw new IllegalStateException("received no streams for topic: " + _config.getConsumerTopic());
        }

        LOG.info("connect to Kafka broker");
        LOG.debug("received " + streams.size() + " stream(s) for topic: " + _config.getConsumerTopic());
        return streams.get(0);
    }

    /**
     * Starts a thread with the ParstreamConsumer continuously listening to
     * messages from a Kafka server, decoding the messages and inserting into
     * ParStream.
     */
    @Override
    public void run() {
        LOG.info("start ParStream Kafka consumer");
        try {
            // set-up Kafka's consumer
            _consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(_consumerProperties));

            // set-up ParStream database connection
            connectParstream();
            MessageDecoder decoder = _decoderFactory.create(_psConnection.listImportColumns(_config.getTableName()));

            // loop reading messages from Kafka broker
            ConsumerIterator<byte[], byte[]> it = subscribeTopic().iterator();
            startCommitTimer();
            while (!_shutdown && it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> msg = it.next();
                insertRows(decoder.decodeMessage(msg));
                LOG.debug("processed Kafka message");
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                LOG.debug("caught an InterruptedException", e);
            } else if (e instanceof ParstreamException || e instanceof ParstreamFatalException) {
                LOG.fatal("Unrecoverable ParStream exception encountered", e);
            } else {
                LOG.fatal("Unexpected exception encountered", e);
            }
        } finally {
            if (!_shutdown) {
                shutdown();
            }
        }
        LOG.info("end ParStream Kafka consumer");
    }

    /**
     * Commit any pending rows, clean up resources.
     * 
     */
    protected void shutdown() {
        synchronized (_syncLock) {
            try {
                // try to commit pending rows
                commit(CommitMode.SHUTDOWN_MODE);
            } catch (ParstreamFatalException e) {
                LOG.error(e.getMessage());
            } catch (ParstreamException e) {
                // invalid ParStream table name, nothing to be done
                LOG.debug("caught a ParstreamException", e);
            }

            // end timer thread
            if (_timer != null) {
                _timer.cancel();
            }

            LOG.info("close ParStream connection");
            if (_psConnection != null) {
                _psConnection.close();
            }

            LOG.info("close Kafka broker connection");
            if (_consumer != null) {
                _consumer.shutdown();
            }
        }
    }

    private void insertRows(List<Object[]> rows) throws ParstreamFatalException, ParstreamException {
        synchronized (_syncLock) {
            for (Object[] row : rows) {
                try {
                    _psConnection.rawInsert(row);
                    ++_pendingRows;
                } catch (ParstreamException e) {
                    LOG.error(e.getMessage());
                }

                if (_config.getCommitRateRowCount() != null && _pendingRows >= _config.getCommitRateRowCount()) {
                    commit(CommitMode.AUTOCOMMIT_ROWCOUNT);
                }
            }
        }
    }

    private void commit(CommitMode commitMode) throws ParstreamFatalException, ParstreamException {
        if (_timerTask != null) {
            _timerTask.cancel();
        }

        // log used commit mode
        switch (commitMode) {
        case AUTOCOMMIT_ROWCOUNT:
            LOG.info("auto-commit row count reached");
            break;

        case AUTOCOMMIT_TIMESPAN:
            LOG.info("auto-commit time span reached");
            break;

        case SHUTDOWN_MODE:
            LOG.info("commiting pending rows before shutdown");
            break;
        }

        // commit messages in configured order
        switch (_config.getCommitOrder()) {
        case CONSUMER_PARSTREAM:
            commitConsumer();
            commitParstream();
            break;
        case PARSTREAM_CONSUMER:
            commitParstream();
            commitConsumer();
            break;
        }
        _pendingRows = 0;

        // restart timer task for auto commit
        switch (commitMode) {
        case AUTOCOMMIT_ROWCOUNT:
        case AUTOCOMMIT_TIMESPAN:
            _psConnection.prepareInsert(_config.getTableName());
            startCommitTimer();
            break;

        case SHUTDOWN_MODE:
            break;
        }
    }

    private void commitParstream() throws ParstreamFatalException {
        if (_psConnection != null) {
            LOG.info("will commit " + _pendingRows + " row(s) to ParStream");
            _psConnection.commit();
        }
    }

    private void commitConsumer() {
        if (_consumer != null) {
            LOG.info("commiting consumer offsets to kafka");
            _consumer.commitOffsets();
        }
    }

    /**
     * Used by the applications' shut down hook.
     */
    protected void enableShutdownMode() {
        _shutdown = true;
    }
}
