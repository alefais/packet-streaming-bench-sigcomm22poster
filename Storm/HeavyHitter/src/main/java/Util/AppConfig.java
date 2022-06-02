/*
 * Copyright (C) 2022 Università di Pisa
 * Copyright (C) 2022 Alessandra Fais
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *   1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *   2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

 /**
 * @author Alessandra Fais
 * @version 18/05/2022
 *
 * Class for retrieving and managing application configuration properties.
 */
package Util;

import org.apache.commons.lang.math.NumberUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AppConfig {

    /**
     * Internal representation of the property list as a set of key-value pairs.
     */
    private final HashMap<String, Object> config;

    /**
     * Constructor.
     */
    public AppConfig() {
        config = new HashMap<>();
    }

    /**
     * Checks if the value corresponding to the given key is of type Integer.
     * @param str key
     * @return true if it is an Integer, false otherwise
     */
    private boolean isInteger(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c <= '/' || c >= ':') {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets the value corresponding to the given key, if it is of type String.
     * @param key key
     * @return value if it is not null and if it is an Object of type String
     */
    public String getString(String key) {
        String val = null;
        Object obj = this.config.get(key);
        if (null != obj) {
            if (obj instanceof String){
                val = (String)obj;
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    /**
     * Gets the value corresponding to the given key, if it is of type Integer.
     * @param key key
     * @return value if it is not null and if it is an Object of type Integer
     */
    public int getInt(String key) {
        int val = 0;
        Object obj = this.config.get(key);

        if (null != obj) {
            if (obj instanceof Integer) {
                val = (Integer)obj;
            } else if (obj instanceof Number) {
                val = ((Number)obj).intValue();
            } else if (obj instanceof String) {
                try {
                    val = Integer.parseInt((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Value for configuration key " + key + " cannot be parsed to an Integer", ex);
                }
            } else {
                throw new IllegalArgumentException("Integer value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }

        return val;
    }

    /**
     * Gets the value corresponding to the given key, if it is of type Long.
     * @param key key
     * @return value if it is not null and if it is an Object of type Long
     */
    public long getLong(String key) {
        long val = 0;
        Object obj = this.config.get(key);
        if (null != obj) {
            if (obj instanceof Long) {
                val = (Long)obj;
            } else if (obj instanceof String) {
                try {
                    val = Long.parseLong((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found  in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    /**
     * Gets the value corresponding to the given key, if it is of type Double.
     * @param key key
     * @return value if it is not null and if it is an Object of type Double
     */
    public double getDouble(String key) {
        double val = 0;
        Object obj = this.config.get(key);
        if (null != obj) {
            if (obj instanceof Double) {
                val = (Double)obj;
            } else if (obj instanceof String) {
                try {
                    val = Double.parseDouble((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    /**
     * Gets the value corresponding to the given key, if it is of type Boolean.
     * @param key key
     * @return value if it is not null and if it is an Object of type Boolean
     */
    public boolean getBoolean(String key) {
        boolean val = false;
        Object obj = this.config.get(key);
        if (null != obj) {
            if (obj instanceof Boolean) {
                val = (Boolean)obj;
            } else if (obj instanceof String) {
                val = Boolean.parseBoolean((String)obj);
            } else {
                throw new IllegalArgumentException("Boolean value not found  in configuration  for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    /**
     * Gets the value corresponding to the given key, if it is a vector of Integers.
     * Uses "," as default separator.
     * @param key key
     * @param def default value
     * @return array of int values if it is not null
     */
    public int[] getIntArray(String key, int[] def) {
        return getIntArray(key, ",", def);
    }

    /**
     * Gets the value corresponding to the given key, if it is a vector of Integers.
     * @param key key
     * @param separator string separating the values
     * @param def default value
     * @return array of int values if it is not null
     */
    public int[] getIntArray(String key, String separator, int[] def) {
        int[] values = null;

        try {
            values = getIntArray(key, separator);
        } catch (IllegalArgumentException ex) {
            values = def;
        }

        return values;
    }

    /**
     * Gets the value corresponding to the given key, if it is a vector of Integers.
     * @param key key
     * @param separator string separating the values
     * @return array of int values if it is not null
     */
    private int[] getIntArray(String key, String separator) {
        String value = getString(key);
        String[] items = value.split(separator);
        int[] values = new int[items.length];

        for (int i = 0; i < items.length; i++) {
            try {
                values[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Value for configuration key "
                                + key + " cannot be parsed to an Integer array", ex);
            }
        }

        return values;
    }

    /**
     * Checks if the given key exists in the map of app configuration properties.
     * @param key key
     * @return true if the key exists, false otherwise
     */
    public boolean exists(String key) {
        return this.config.containsKey(key);
    }

    /**
     * Updates the internal state of the current AppConfig object
     * from the app configuration properties contained in a Map collection.
     * @param map collection of properties
     */
    public void fromMap(Map map) {
        for (Object k : map.keySet()) {
            String key = (String) k;
            Object value = map.get(key);

            if (value instanceof String) {
                String str = (String) value;
                if (isInteger(str)) {
                    this.config.put(key, Integer.parseInt(str));
                } else if (NumberUtils.isNumber(str)) {
                    this.config.put(key, Double.parseDouble(str));
                } else if (value.equals("true") || value.equals("false")) {
                    this.config.put(key, Boolean.parseBoolean(str));
                } else {
                    this.config.put(key, value);
                }
            } else {
                this.config.put(key, value);
            }
        }
    }

    /**
     * Updates the internal state of the current AppConfig object
     * from the app configuration properties contained in a Properties collection.
     * @param properties collection of properties
     */
    public void fromProperties(Properties properties) {
        for (String key : properties.stringPropertyNames()) {
            this.config.put(key, parseString(properties.getProperty(key)));
        }
    }

    /**
     * Updates the internal state of the current AppConfig object
     * from the app configuration properties contained in a String object.
     * @param str collection of properties
     */
    public void fromStr(String str) {
        Map<String, String> map = strToMap(str);

        for (String key : map.keySet()) {
            this.config.put(key, parseString(map.get(key)));
        }
    }

    /**
     * Creates a Map collection of properties from the content of a String object.
     * @param str collection of properties in the form of a string
     * @return collection of properties in a Map data structure
     */
    private Map<String, String> strToMap(String str) {
        Map<String, String> map = new HashMap<>();
        String[] arguments = str.split(",");

        for (String arg : arguments) {
            String[] kv = arg.split("=");
            map.put(kv[0].trim(), kv[1].trim());
        }

        return map;
    }

    /**
     * Converts a string value into the right king of Object.
     * @param value initial string value
     * @return converted Object
     */
    private Object parseString(String value) {
        if (isInteger(value)) {
            return Integer.parseInt(value);
        } else if (NumberUtils.isNumber(value)) {
            return Double.parseDouble(value);
        } else if (value.equals("true") || value.equals("false")) {
            return Boolean.parseBoolean(value);
        }
        return value;
    }
}