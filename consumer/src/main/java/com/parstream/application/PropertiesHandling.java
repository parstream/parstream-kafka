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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Properties loading and merging handling helper.
 */
public class PropertiesHandling {

    private static final Log LOG = LogFactory.getLog(PropertiesHandling.class);

    /**
     * Merge properties from a resource file.
     *
     * @param properties
     *            Properties to be merged
     * @param name
     *            Name of properties resource file
     * @throws IOException
     */
    public static void mergePropertiesResource(Properties properties, String name) throws IOException {
        try (InputStream is = PropertiesHandling.class.getClassLoader().getResourceAsStream(name)) {
            if (is != null) {
                LOG.debug("load default properties " + name + " from classpath");
                properties.load(is);
            }
        }
    }

    /**
     * Merge properties from an external file.
     *
     * @param properties
     *            Properties to be merged
     * @param file
     *            external properties file
     * @throws IOException
     */
    public static void mergePropertiesFile(Properties properties, File file) throws IOException {
        if (file.exists()) {
            try (InputStream is = new FileInputStream(file)) {
                LOG.debug("load external properties " + file.getName());
                properties.load(is);
            }
        }
    }

    /**
     * Load and merge properties in three stages: from classpath, external file
     * from working directory and external file from user's home directory.
     * <p>
     * Exceptions while file access will be ignored and logged as error. The
     * default properties resource must be exist on the classpath.
     * 
     * @param name
     *            name of the file
     * @return merged properties
     */
    public static Properties loadProperties(String name) {
        Properties properties = new Properties();

        /* load bundled properties as default */
        try {
            mergePropertiesResource(properties, name);
        } catch (IOException ex) {
            LOG.error("exception while reading resource " + name, ex);
        }

        /* load external properties from user's home directory */
        String userHome = System.getProperty("user.home");
        try {
            mergePropertiesFile(properties, new File(userHome, name));
        } catch (IOException ex) {
            LOG.error("exception while reading resource " + name, ex);
        }

        /* load external properties from the current working directory */
        String cwd = System.getProperty("user.dir");
        try {
            if (!cwd.equals(userHome)) {
                mergePropertiesFile(properties, new File(name));
            }
        } catch (IOException ex) {
            LOG.error("exception while reading resource " + name, ex);
        }
        return properties;
    }
}
