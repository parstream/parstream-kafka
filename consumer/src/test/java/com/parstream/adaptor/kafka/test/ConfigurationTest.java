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
package com.parstream.adaptor.kafka.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.parstream.adaptor.kafka.Configuration;
import com.parstream.application.PropertiesHandling;

public class ConfigurationTest {

    private Properties _minProperties;
    private Properties _maxProperties;
    private Properties _consumerProperties;

    @Before
    public void loadProperties() throws FileNotFoundException, IOException {
        _minProperties = new Properties();
        _minProperties.load(new FileInputStream(new File("target/classes/database.properties")));
        PropertiesHandling.mergePropertiesFile(_minProperties, new File(
                "target/test-classes/MinConfiguration.properties"));

        _maxProperties = new Properties();
        _maxProperties.load(new FileInputStream(new File("target/classes/database.properties")));
        PropertiesHandling.mergePropertiesFile(_maxProperties, new File(
                "target/test-classes/MaxConfiguration.properties"));

        _consumerProperties = new Properties();
        _consumerProperties.load(new FileInputStream(new File("target/test-classes/consumer.properties")));
    }

    @Test
    public void testMaxValidConfiguration() throws Exception {
        Configuration psConf = new Configuration(_maxProperties, _consumerProperties);

        assertEquals("localhost", psConf.getDatabaseHost());
        assertEquals("1", psConf.getDatabasePort());
        assertEquals(1, psConf.getDatabaseTimeout());
        assertEquals("user", psConf.getDatabaseUsername());
        assertEquals("pass", psConf.getDatabasePassword());
        assertEquals("abc", psConf.getTableName());
        assertEquals(Configuration.CommitOrder.PARSTREAM_CONSUMER, psConf.getCommitOrder());
        assertEquals(new File("/path/to/mapping/file"), psConf.getDecoderMappingFile());
        assertEquals(2L, psConf.getCommitRateRowCount().longValue());
        assertEquals(1L, psConf.getCommitRateTimeSpan().longValue());
        assertEquals(TestMessageDecoder.class, psConf.getDecoderClass());
    }

    @Test
    public void testMinValidConfiguration() throws Exception {
        Configuration psConf = new Configuration(_minProperties, _consumerProperties);

        assertEquals("localhost", psConf.getDatabaseHost());
        assertEquals("1", psConf.getDatabasePort());
        assertEquals(0, psConf.getDatabaseTimeout());
        assertEquals(null, psConf.getDatabaseUsername());
        assertEquals(null, psConf.getDatabasePassword());
        assertEquals("abc", psConf.getTableName());
        assertEquals(Configuration.CommitOrder.CONSUMER_PARSTREAM, psConf.getCommitOrder());
        assertEquals(new File("/path/to/mapping/file"), psConf.getDecoderMappingFile());
        assertEquals(2L, psConf.getCommitRateRowCount().longValue());
        assertEquals(null, psConf.getCommitRateTimeSpan());
        assertEquals(TestMessageDecoder.class, psConf.getDecoderClass());
    }

    @Test
    public void testKafkaAutocommit() {
        _consumerProperties.setProperty("auto.commit.enable", "true");
        Configuration config = new Configuration(_maxProperties, _consumerProperties);
        assertEquals(config.getConsumerProperties().get("auto.commit.enable"), "false");
    }

    @Test
    public void testMissingHost() throws Exception {
        _minProperties.remove("parstream.server.address");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Missing parstream host did not yield expected error message");
        } catch (NullPointerException expected) {
            assertEquals("No Parstream server host specified. Use configuration key: parstream.server.address",
                    expected.getMessage());
        }
    }

    @Test
    public void testMissingPort() throws Exception {
        _minProperties.remove("parstream.server.port");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Missing parstream port did not yield expected error message");
        } catch (NullPointerException expected) {
            assertEquals("No Parstream server port specified. Use configuration key: parstream.server.port",
                    expected.getMessage());
        }
    }

    @Test
    public void testMissingDecoderType() throws Exception {
        _minProperties.remove("parstream.decoder.type");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Missing parstream decoder type did not yield expected error message");
        } catch (NullPointerException expected) {
            assertEquals("No Parstream decoder type specified. Use configuration key: parstream.decoder.type",
                    expected.getMessage());
        }
    }

    @Test
    public void testMissingDecoderFilePath() throws Exception {
        _minProperties.remove("parstream.decoder.mappingfile");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Missing parstream decoder file path did not yield expected error message");
        } catch (NullPointerException expected) {
            assertEquals(
                    "No Parstream decoder mapping file specified. Use configuration key: parstream.decoder.mappingfile",
                    expected.getMessage());
        }
    }

    @Test
    public void testMissingTableName() throws Exception {
        _minProperties.remove("parstream.import.table");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Missing parstream table name did not yield expected error message");
        } catch (NullPointerException expected) {
            assertEquals("No Parstream table name specified. Use configuration key: parstream.import.table",
                    expected.getMessage());
        }
    }

    @Test
    public void testParstreamConsumerCommitOrder() {
        _minProperties.setProperty("parstream.commit.order", "Parstream,Consumer");
        Configuration psConf = new Configuration(_minProperties, _consumerProperties);
        assertEquals("Parstream then Consumer commit order", Configuration.CommitOrder.PARSTREAM_CONSUMER,
                psConf.getCommitOrder());
    }

    @Test
    public void testConsumerParstreamCommitOrder() {
        _minProperties.setProperty("parstream.commit.order", "Consumer,Parstream");
        Configuration psConf = new Configuration(_minProperties, _consumerProperties);
        assertEquals("Consumer then Parstream commit order", Configuration.CommitOrder.CONSUMER_PARSTREAM,
                psConf.getCommitOrder());
    }

    @Test
    public void testCommitOrderIncomplete() {
        _minProperties.setProperty("parstream.commit.order", "Parstream");
        try {
            Configuration psConf = new Configuration(_minProperties, _consumerProperties);
            assertEquals("Default commit order", Configuration.CommitOrder.CONSUMER_PARSTREAM, psConf.getCommitOrder());
            fail("commit order is incomplete");
        } catch (IllegalArgumentException expected) {
            assertEquals(expected.getMessage(), "No valid transaction commit strategy defined.");
        }
    }

    @Test
    public void testCommitOrderInvalidConsumer() {
        _minProperties.setProperty("parstream.commit.order", "Consumer,ParStream");
        try {
            Configuration psConf = new Configuration(_minProperties, _consumerProperties);
            assertEquals("Default commit order", Configuration.CommitOrder.CONSUMER_PARSTREAM, psConf.getCommitOrder());
            fail("commit order is incomplete");
        } catch (IllegalArgumentException expected) {
            assertEquals(expected.getMessage(), "No valid transaction commit strategy defined.");
        }
    }

    @Test
    public void testCommitStrategyTimespan() {
        _minProperties.remove("parstream.autocommit.rowcount");
        _minProperties.setProperty("parstream.autocommit.timespan", "3");
        Configuration config = new Configuration(_minProperties, _consumerProperties);
        assertEquals(new Long(3), config.getCommitRateTimeSpan());
    }

    @Test
    public void testMissingCommitStrategy() {
        _minProperties.remove("parstream.autocommit.rowcount");
        _minProperties.remove("parstream.autocommit.timespan");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Missing parstream commit strategy did not yield expected error message");
        } catch (IllegalArgumentException expected) {
            assertEquals(
                    "No Parstream commit strategy defined. Use either configuration key: parstream.autocommit.rowcount, or configuration key: parstream.autocommit.timespan",
                    expected.getMessage());
        }
    }

    @Test
    public void testNonNumericCommitRowCount() throws Exception {
        _minProperties.setProperty("parstream.autocommit.rowcount", "abc");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Non numeric commit row count did not yield expected error message");
        } catch (IllegalArgumentException expected) {
            assertEquals("Specified Parstream commit row count must be a numerical value", expected.getMessage());
        }
    }

    @Test
    public void testZeroCommitRowCount() throws Exception {
        _minProperties.setProperty("parstream.autocommit.rowcount", "0");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("0 commit row count did not yield expected error message");
        } catch (IllegalArgumentException expected) {
            assertEquals("Specified Parstream commit row count must be a positive value", expected.getMessage());
        }
    }

    @Test
    public void testNonNumericCommitTimeSpan() throws Exception {
        _minProperties.setProperty("parstream.autocommit.timespan", "abc");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Non numeric commit row count did not yield expected error message");
        } catch (IllegalArgumentException expected) {
            assertEquals("Specified Parstream commit time span must be a numerical value", expected.getMessage());
        }
    }

    @Test
    public void testZeroCommitTimeSpan() {
        _minProperties.setProperty("parstream.autocommit.timespan", "0");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("0 commit time span did not yield expected error message");
        } catch (IllegalArgumentException expected) {
            assertEquals("Specified Parstream commit time span must be a positive value", expected.getMessage());
        }
    }

    @Test
    public void testMissingTimeout() {
        Configuration psConf = new Configuration(_minProperties, _consumerProperties);
        assertEquals("Missing connection timeout did not set default value", psConf.getDatabaseTimeout(), 0L);
    }

    @Test
    public void testNonNumericTimeout() {
        _minProperties.setProperty("parstream.server.timeout", "abc");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("Non numeric timeout did not yield expected error message");
        } catch (IllegalArgumentException expected) {
            assertEquals("Specified Parstream connection timeout must be a numerical value", expected.getMessage());
        }
    }

    @Test
    public void testInvalidDecoderType() {
        _minProperties.setProperty("parstream.decoder.type", "some.random.decoder.type");
        try {
            new Configuration(_minProperties, _consumerProperties);
            fail("No exception thrown with invalid decoder class name");
        } catch (IllegalArgumentException expected) {
            assertEquals("Invalid Parstream decoder specified.", expected.getMessage());
        }
    }

    @Test
    public void testKafkaTopic() {
        _consumerProperties.setProperty("topic", "mytopic");
        Configuration config = new Configuration(_minProperties, _consumerProperties);
        assertEquals(config.getConsumerTopic(), "mytopic");
    }
}
