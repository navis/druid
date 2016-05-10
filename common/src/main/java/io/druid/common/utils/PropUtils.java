/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import java.util.Map;
import java.util.Properties;

import io.druid.java.util.common.ISE;

/**
 */
public class PropUtils
{
  public static String getProperty(Properties props, String property)
  {
    String retVal = props.getProperty(property);

    if (retVal == null) {
      throw new ISE("Property[%s] not specified.", property);
    }

    return retVal;
  }

  public static int getPropertyAsInt(Properties props, String property)
  {
    return getPropertyAsInt(props, property, null);
  }

  public static int getPropertyAsInt(Properties props, String property, Integer defaultValue)
  {
    String retVal = props.getProperty(property);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new ISE("Property[%s] not specified.", property);
      } else {
        return defaultValue;
      }
    }

    try {
      return Integer.parseInt(retVal);
    }
    catch (NumberFormatException e) {
      throw new ISE(e, "Property[%s] is expected to be an int, it is not[%s].", property, retVal);
    }
  }

  public static String parseString(Map<String, ?> context, String key, String defaultValue)
  {
    if (context == null) {
      return defaultValue;
    }
    Object val = context.get(key);
    if (val == null) {
      return defaultValue;
    }
    return String.valueOf(val);
  }

  public static int parseInt(Map<String, ?> context, String key, int defaultValue)
  {
    if (context == null) {
      return defaultValue;
    }
    Object val = context.get(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Integer.parseInt((String) val);
    } else if (val instanceof Number) {
      return ((Number) val).intValue();
    } else {
      throw new ISE("Unknown type [%s]", val.getClass());
    }
  }

  public static boolean parseBoolean(Map<String, ?> context, String key, boolean defaultValue)
  {
    if (context == null) {
      return defaultValue;
    }
    Object val = context.get(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Boolean.parseBoolean((String) val);
    } else if (val instanceof Boolean) {
      return (boolean) val;
    } else {
      throw new ISE("Unknown type [%s]. Cannot parse!", val.getClass());
    }
  }
}
