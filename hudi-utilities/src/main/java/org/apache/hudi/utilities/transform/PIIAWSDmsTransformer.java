/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.transform;

import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * A Simple transformer that adds `Op` field with value `I`, for AWS DMS data, if the field is not
 * present.
 */
public class PIIAWSDmsTransformer implements Transformer {

  public static final String BLACKLIST_ENTRY = "COMMON_PII_COLS";
  public static final String WHITELIST_CONFIG_PATH = "/whitelist.json";
  public static final String BLACKLIST_CONFIG_PATH = "/blacklist.json";

  public static final String[] HUDI_COLUMNS = {"update_timestamp", "Op"};

  private HashSet<String> whitelistColumnNames;
  private HashSet<String> blacklistColumnNames;
  private String databaseName;
  private String tableName;
  private boolean configsLoaded = false;

  /**
   * Configs supported
   */
  static class Config {

    private static final String DATABASE_NAME = "hoodie.deltastreamer.database";
    private static final String TABLE_NAME = "hoodie.deltastreamer.table";
  }

  /**
   * Load in the configuration files to know which columns to keep / remove.
   */
  private void loadConfigs(TypedProperties properties) throws IOException, IllegalArgumentException, ParseException {

    // get DB / table names from the environment variables
    // Map<String, String> systemEnvironment = System.getenv();
    // if (!systemEnvironment.containsKey(DB_NAME_ENV) || !systemEnvironment.containsKey(TABLE_NAME_ENV)) {
    //   throw new IllegalArgumentException("Missing database or table environment variables.");
    // } else {
    //   databaseName = systemEnvironment.get(DB_NAME_ENV);
    //   tableName = systemEnvironment.get(TABLE_NAME_ENV);
    // }

    // get DB / table names from the properties file
    databaseName = properties.getString(Config.DATABASE_NAME);
    tableName = properties.getString(Config.TABLE_NAME);

    // load in config files
    JSONParser parser = new JSONParser();
    JSONObject allWhitelistObject = null;
    JSONObject databaseBlacklistObject = null;

    // there should be config files describing both a whitelist and a blacklist
    InputStream whitelistStream = getClass().getResourceAsStream(WHITELIST_CONFIG_PATH);
    InputStream blacklistStream = getClass().getResourceAsStream(BLACKLIST_CONFIG_PATH);
    if (whitelistStream == null || blacklistStream == null) {
      throw new IllegalArgumentException("Missing config resources");
    }

    try {
      allWhitelistObject = (JSONObject) parser.parse(new InputStreamReader(whitelistStream));
      databaseBlacklistObject = (JSONObject) parser.parse(new InputStreamReader(blacklistStream));
    } catch (ParseException e) {
      throw e;
    }

    // get whitelist list for the table
    try {
      JSONObject databaseWhitelistObject = (JSONObject) allWhitelistObject.get(databaseName);
      whitelistColumnNames = new HashSet<String>((List<String>)databaseWhitelistObject.get(tableName));
      for (String hudiColumn : HUDI_COLUMNS) {
        whitelistColumnNames.add(hudiColumn);
      }
    } catch (Exception e) {
      // if there is no entry, ignore it
      whitelistColumnNames = null;
    }

    // get blacklist list for all tables
    if (!databaseBlacklistObject.containsKey(BLACKLIST_ENTRY)) {
      throw new IllegalArgumentException("Missing list of permenantly blacklisted columns.");
    } else {
      blacklistColumnNames = new HashSet<String>((List<String>)databaseBlacklistObject.get(BLACKLIST_ENTRY));
    }

  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
      TypedProperties properties) {

    // load config files (table / db name, whitelisted / blacklisted column names)
    if (!configsLoaded) {
      try {
        loadConfigs(properties);
        configsLoaded = true;
      } catch (Exception e) {
        // end deltastreamer on fatal error
        e.printStackTrace();
        System.exit(0);
      }
    }

    // add Op column
    Option<String> opColumnOpt = Option.fromJavaOptional(
        Arrays.stream(rowDataset.columns()).filter(c -> c.equals("Op")).findFirst());
    if (!opColumnOpt.isPresent()) {
      rowDataset = rowDataset.withColumn("Op", lit(""));
    }

    // Remove all columns that are always PII
    for (String name : blacklistColumnNames) {
      rowDataset = rowDataset.drop(name);
    }

    // make sure all remaining columns are whitelisted
    // (if an entry for this table existed)
    if (whitelistColumnNames != null) {
      String[] allColumnNames = rowDataset.columns();
      for (String name : allColumnNames) {
        if (!whitelistColumnNames.contains(name)) {
          rowDataset = rowDataset.drop(name);
        }
      }
    }

    return rowDataset;
  }
}
