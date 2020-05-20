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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hudi.utilities.transform.PIIAWSDmsTransformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

public class TestPIIAWSDmsTransformer {

  private static final String SAMPLE_DB = "/dummy_db_PII_transformer_test_unique_name.parquet";
  private static final String SAMPLE_DB_FILE_LOCATION = String.format("/tmp/%s", SAMPLE_DB);

  private static boolean setUpIsDone = false;
  private static SparkSession spark = null;
  private static Dataset<Row> sampleDF = null;

  private void columnsEqualityCheck(List<String> expectedColumns, List<String> foundColumns) {
    for (String columnName : expectedColumns) {
      assertTrue(foundColumns.contains(columnName));
    }
    for (String columnName : foundColumns) {
      assertTrue(expectedColumns.contains(columnName));
    }
  }

  @Before
  public void setUp() {

    spark = SparkSession.builder()
      .appName("PII Transformer Unit Testing")
      .config("spark.master", "local")
      .getOrCreate();

    byte[] tmpBuffer = null;
    try {
      // load in sample database
      InputStream dummyDatabaseStream = getClass().getResourceAsStream(SAMPLE_DB);
      tmpBuffer = new byte[dummyDatabaseStream.available()];
      dummyDatabaseStream.read(tmpBuffer);
    } catch (Exception e) {
      fail("Failed to load sampleDB. Likely missing sample DB file in resources.");
    }

    try {
      // write to /tmp for spark to read from
      File databaseFile = new File(SAMPLE_DB_FILE_LOCATION);
      OutputStream databaseOutputStream = new FileOutputStream(databaseFile);
      databaseOutputStream.write(tmpBuffer);
    } catch (Exception e) {
      fail("Failed to write sampleDB to /tmp. Likely permission issues.");
    }

    // Sample file has following columns:
    sampleDF = spark.read().parquet(SAMPLE_DB_FILE_LOCATION);

    String[] initialColumns = {"whitelisted", "blacklisted",
                               "whitelisted_and_blacklisted",
                               "ignored",
                               "update_timestamp"};

    // check initial status
    columnsEqualityCheck(Arrays.asList(initialColumns), Arrays.asList(sampleDF.columns()));

    setUpIsDone = true;
  }

  @AfterClass
  public static void cleanUp() {
    spark.stop();
  }

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testValidColumnRemoval() {

    // set dummy database / table names
    environmentVariables.set(PIIAWSDmsTransformer.DB_NAME_ENV, "dummy_db");
    environmentVariables.set(PIIAWSDmsTransformer.TABLE_NAME_ENV, "dummy_table");

    PIIAWSDmsTransformer transformer = new PIIAWSDmsTransformer();

    // apply transformer to remove columns not specified in src/test/resources/whitelist.json
    Dataset<Row> transformedDF = transformer.apply(null, null, sampleDF, null);

    String[] whitelistedColumns = {"whitelisted", "Op", "update_timestamp"};

    // we expect the two columns found in whitelist.json for dummy_table_valid + Op + updated_at
    columnsEqualityCheck(Arrays.asList(transformedDF.columns()), Arrays.asList(whitelistedColumns));

  }

  @Test
  public void testNoEntry() {

    // set dummy database / table names
    environmentVariables.set(PIIAWSDmsTransformer.DB_NAME_ENV, "dummy_db");
    environmentVariables.set(PIIAWSDmsTransformer.TABLE_NAME_ENV, "missing_table");

    PIIAWSDmsTransformer transformer = new PIIAWSDmsTransformer();

    // apply transformer to remove columns not specified in src/test/resources/whitelist.json
    Dataset<Row> transformedDF = transformer.apply(null, null, sampleDF, null);

    String[] whitelistedColumns = {"whitelisted", "Op", "update_timestamp", "ignored"};

    // we expect everything except for blacklisted column
    columnsEqualityCheck(Arrays.asList(transformedDF.columns()), Arrays.asList(whitelistedColumns));

  }

  @Test
  public void testEmptyEntry() {

    // set dummy database / table names
    environmentVariables.set(PIIAWSDmsTransformer.DB_NAME_ENV, "dummy_db");
    environmentVariables.set(PIIAWSDmsTransformer.TABLE_NAME_ENV, "dummy_table_empty");

    PIIAWSDmsTransformer transformer = new PIIAWSDmsTransformer();

    // apply transformer to remove columns not specified in src/test/resources/whitelist.json
    Dataset<Row> transformedDF = transformer.apply(null, null, sampleDF, null);

    String[] whitelistedColumns = {"Op", "update_timestamp"};

    // we expect nothing except for hudi fields
    columnsEqualityCheck(Arrays.asList(transformedDF.columns()), Arrays.asList(whitelistedColumns));

  }

  @Test
  public void testMissingEnvironmentVariables() {

    // expect to cause a System.exit()
    exit.expectSystemExitWithStatus(0);
    PIIAWSDmsTransformer transformer = new PIIAWSDmsTransformer();
    Dataset<Row> transformedDF = transformer.apply(null, null, sampleDF, null);

  }

}
