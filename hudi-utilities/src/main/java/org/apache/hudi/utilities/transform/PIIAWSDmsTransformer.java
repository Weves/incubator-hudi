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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.payload.CustomAWSDmsAvroPayload;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.io.FileReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import static org.apache.spark.sql.functions.lit;

/**
 * A Simple transformer that adds `Op` field with value `I`, for AWS DMS data, if the field is not
 * present.
 */
public class PIIAWSDmsTransformer implements Transformer {

  private static final String DB_NAME_ENV = "DATABASE_NAME";
  private static final String TABLE_NAME_ENV = "TABLE_NAME";
  private static final String BLACKLIST_ENTRY = "COMMON_PII_COLS";
  private static final String WHITELIST_CONFIG_PATH = "/Users/chris.weaver/Downloads/whitelist.json";
  private static final String BLACKLIST_CONFIG_PATH = "/Users/chris.weaver/Downloads/blacklist.json";

  // private static final String[] HUDI_COLUMNS = {"_hoodie_commit_time", "_hoodie_commit_seqno",
  //                                               "_hoodie_record_key", "_hoodie_partition_path",
  //                                               "_hoodie_file_name", "update_timestamp"};
  private static final String[] HUDI_COLUMNS = {"update_timestamp", "Op"};

  private HashSet<String> whitelistColumnNames;
  private HashSet<String> blacklistColumnNames;
  private String databaseName;
  private String tableName;
  private boolean configsLoaded = false;

  /**
   * Load in the configuration files to know which columns to keep / remove.
   */
  private void loadConfigs() {

    // get DB / table names from the environment variables
    try {
      databaseName = System.getenv(DB_NAME_ENV);
      tableName = System.getenv(TABLE_NAME_ENV);
    } catch (Exception e) {
      // there should be environment variables
      e.printStackTrace();
      System.exit(0);
    }

    // load in config files
    JSONParser parser = new JSONParser();
    JSONObject allWhitelistObject = null;
    JSONObject databaseBlacklistObject = null;
    try {
      allWhitelistObject = (JSONObject) parser.parse(new FileReader(WHITELIST_CONFIG_PATH));
      databaseBlacklistObject = (JSONObject) parser.parse(new FileReader(BLACKLIST_CONFIG_PATH));
    } catch (Exception e) {
      // there should be config files
      e.printStackTrace();
      System.exit(0);
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
    try {
      blacklistColumnNames = new HashSet<String>((List<String>)databaseBlacklistObject.get(BLACKLIST_ENTRY));
    } catch (Exception e) {
      // there should be a blacklist list
      e.printStackTrace();
      System.exit(0);
    }

  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
      TypedProperties properties) {

    // load config files (table / db name, whitelisted / blacklisted column names)
    if (!configsLoaded) {
      loadConfigs();
      configsLoaded = true;
    }

    // add Op column
    Option<String> opColumnOpt = Option.fromJavaOptional(
        Arrays.stream(rowDataset.columns()).filter(c -> c.equals(CustomAWSDmsAvroPayload.OP_FIELD)).findFirst());
    if (!opColumnOpt.isPresent()) {
      rowDataset = rowDataset.withColumn(CustomAWSDmsAvroPayload.OP_FIELD, lit(""));
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
