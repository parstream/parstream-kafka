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
package com.parstream.application;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

/**
 * Log4j configuration helper class.
 */
public class Log4jConfiguration {

    private static final Log LOG = LogFactory.getLog(Log4jConfiguration.class);
    static private final String LOG4J_CONFIG_NAME = "log4j.properties";

    /**
     * Reload the log4j configuration by merging additional external
     * configuration.
     */
    public static void reloadConfigutation() {
        Properties properties = new Properties();

        try {
            /* 1st try to load from current working directory */
            PropertiesHandling.mergePropertiesFile(properties, new File(LOG4J_CONFIG_NAME));
        } catch (IOException ex) {
            LOG.error("exception while reading resource " + LOG4J_CONFIG_NAME, ex);
        }

        if (properties.isEmpty()) {
            String userHome = System.getProperty("user.home");
            try {
                /* 2nd try to load from user's home directory */
                PropertiesHandling.mergePropertiesFile(properties, new File(userHome, LOG4J_CONFIG_NAME));
            } catch (IOException ex) {
                LOG.error("exception while reading resource " + LOG4J_CONFIG_NAME, ex);
            }
        }

        /* if needed re-load log4j properties */
        if (!properties.isEmpty()) {
            org.apache.log4j.LogManager.resetConfiguration();
            PropertyConfigurator.configure(properties);
            LOG.info("Activated new log4j configuration from '" + LOG4J_CONFIG_NAME + "'");
        }
    }
}
