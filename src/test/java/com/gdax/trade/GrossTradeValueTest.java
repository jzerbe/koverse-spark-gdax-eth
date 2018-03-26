/*
 * Copyright 2016 Koverse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gdax.trade;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.koverse.com.google.common.collect.Lists;
import com.koverse.sdk.data.SimpleRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrossTradeValueTest extends SharedJavaSparkContext {

  @Test
  public void rddTest() {

    // INIT
    SimpleRecord record0 = new SimpleRecord();
    record0.put("price", 574.92000000);
    record0.put("side", "sell");
    record0.put("size", 0.27880799);
    record0.put("trade_id", "31000160");

    SimpleRecord record1 = new SimpleRecord();
    record1.put("price", 574.36000000);
    record1.put("side", "sell");
    record1.put("size", 0.01131300);
    record1.put("trade_id", "31000108");

    JavaRDD<SimpleRecord> inputRecordsRdd = jsc().parallelize(Lists.newArrayList(record0, record1));

    // ACT
    GrossTradeValue grossTradeValue = new GrossTradeValue("price", "size", "trade_id");
    JavaRDD<SimpleRecord> outputRecordsRdd = grossTradeValue.calculateGross(inputRecordsRdd);

    // ASSERT
    assertEquals(outputRecordsRdd.count(), 2);

    List<SimpleRecord> outputRecords = outputRecordsRdd.collect();

    Optional<SimpleRecord> sampleRecordValues = outputRecords.stream()
        .filter(record -> record.get("trade_id").equals("31000108"))
        .findFirst();
    assertTrue(sampleRecordValues.isPresent());
    assertEquals(6.4977345, (float)sampleRecordValues.get().get("gross"), 0.0000001);

  }
}
