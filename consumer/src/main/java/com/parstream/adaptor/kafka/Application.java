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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.parstream.application.Log4jConfiguration;
import com.parstream.application.PropertiesHandling;

/**
 * Main application class. Reads configuration files, then initializes the
 * ParstreamConsumer and runs it.
 *
 */
public class Application {
    private static final Log LOG = LogFactory.getLog(Application.class);

    /** Application's dynamic injector. */
    public Injector injector;

    private Configuration _configuration;
    private ParstreamConsumer _consumer;

    /**
     * Instantiates a ParStream Kafka consumer application.
     * 
     * @param config
     *            the configuration of this application
     * @param consumerProperties
     *            the properties of the Kafka consumer
     */
    public Application(Properties config, Properties consumerProperties) {
        // implicit check properties by constructing Configuration instance
        _configuration = new Configuration(config, consumerProperties);
    }

    private void run() {
        _consumer = injector.getInstance(ParstreamConsumer.class);
        final Thread consumerThread = new Thread(_consumer);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOG.info("try to graceful shutdown consumer");
                _consumer.enableShutdownMode();
                consumerThread.interrupt();
                _consumer.shutdown();
            }
        });

        consumerThread.start();
    }

    /**
     * Kafka consumer application main method.
     *
     * Reads the database and consumer property files, verifies minimum required
     * settings, and launches the ParstreamConsumer.
     * 
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        // re-configure log4j configuration
        Log4jConfiguration.reloadConfigutation();

        // configure the application
        Properties database = PropertiesHandling.loadProperties("database.properties");
        Properties consumer = PropertiesHandling.loadProperties("consumer.properties");
        final Application theApp = new Application(database, consumer);

        // configure dependency injection
        theApp.injector = Guice.createInjector(new AbstractModule() {
            protected void configure() {

                // this application configuration
                bind(Configuration.class).annotatedWith(Names.named("configuration")).toInstance(theApp._configuration);

                // decoder mapping file
                bind(File.class).annotatedWith(Names.named("mapping file")).toInstance(
                        theApp._configuration.getDecoderMappingFile());

                // decoder factory
                install(new FactoryModuleBuilder().implement(MessageDecoder.class,
                        theApp._configuration.getDecoderClass()).build(DecoderFactory.class));
            }
        });

        // start the application
        theApp.run();
    }
}
