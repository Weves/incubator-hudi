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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.payload.AWSDmsAvroPayload;

import org.junit.Test;

public class TestAWSDmsAvroPayload {

  private static final String AVRO_SCHEMA_STRING = "{\"type\": \"record\","
                                                 + "\"name\": \"events\"," + "\"fields\": [ "
                                                 + "{\"name\": \"Key\", \"type\" : \"int\"},"
                                                 + "{\"name\": \"Op\", \"type\": \"string\"}"
                                                 + "]}";

  @Test
  public void testInsert() {

    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("Key", 0);
    record.put("Op", "I");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(record));

    try {
      Option<IndexedRecord> outputPayload = payload.getInsertValue(avroSchema);
      assertTrue((int) outputPayload.get().get(0) == 0);
      assertTrue(outputPayload.get().get(1).toString().equals("I"));
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

  @Test
  public void testUpdate() {

    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord newRecord = new GenericData.Record(avroSchema);
    newRecord.put("Key", 1);
    newRecord.put("Op", "U");

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("Key", 0);
    oldRecord.put("Op", "I");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(newRecord));

    try {
      Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema);
      assertTrue((int) outputPayload.get().get(0) == 1);
      assertTrue(outputPayload.get().get(1).toString().equals("U"));
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

  @Test
  public void testDelete() {

    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord deleteRecord = new GenericData.Record(avroSchema);
    deleteRecord.put("Key", 2);
    deleteRecord.put("Op", "D");

    GenericRecord oldRecord = new GenericData.Record(avroSchema);
    oldRecord.put("Key", 3);
    oldRecord.put("Op", "U");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(deleteRecord));

    try {
      Option<IndexedRecord> outputPayload = payload.combineAndGetUpdateValue(oldRecord, avroSchema);
      // expect nothing to be comitted to table
      assertFalse(outputPayload.isPresent());
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

  @Test
  public void testInsertDelete() {

    Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    GenericRecord deleteRecord = new GenericData.Record(avroSchema);
    deleteRecord.put("Key", 4);
    deleteRecord.put("Op", "D");

    AWSDmsAvroPayload payload = new AWSDmsAvroPayload(Option.of(deleteRecord));

    try {
      Option<IndexedRecord> outputPayload = payload.getInsertValue(avroSchema);
      // expect nothing to be comitted to table
      assertFalse(outputPayload.isPresent());
    } catch (Exception e) {
      fail("Unexpected exception");
    }

  }

}