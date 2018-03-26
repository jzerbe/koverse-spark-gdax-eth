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

import com.koverse.com.google.common.collect.Lists;

import com.koverse.sdk.Version;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.data.SimpleRecord;
import com.koverse.sdk.transform.spark.JavaSparkTransform;
import com.koverse.sdk.transform.spark.JavaSparkTransformContext;

import org.apache.spark.api.java.JavaRDD;

public class GrossTradeValueTransform extends JavaSparkTransform {

  private static final String PRICE_FIELD_NAME_PARAMETER = "priceFieldName";
  private static final String SIZE_FIELD_NAME_PARAMETER = "sizeFieldName";
  private static final String TRADE_ID_FIELD_NAME_PARAMETER = "tradeIdFieldName";

  /**
   * Koverse calls this method to execute your transform.
   *
   * @param context The context of this spark execution.
   * @return The resulting RDD of this transform execution, applied to the output collection.
   */
  @Override
  protected JavaRDD<SimpleRecord> execute(JavaSparkTransformContext context) {

    // This transform assumes there is a single input Data Collection
    String inputCollectionId = context.getInputCollectionIds().get(0);

    // Get the JavaRDD<SimpleRecord> that represents the input Data Collection
    JavaRDD<SimpleRecord> inputRecordsRdd = context.getInputCollectionRdds().get(inputCollectionId);

    // for each Record, inspect the specified text fields and calculate the gross trade value
    final String priceFieldName = context.getParameters().get(PRICE_FIELD_NAME_PARAMETER);
    final String sizeFieldName = context.getParameters().get(SIZE_FIELD_NAME_PARAMETER);
    final String tradeIdFieldName = context.getParameters().get(TRADE_ID_FIELD_NAME_PARAMETER);
    final GrossTradeValue grossTradeValue = new GrossTradeValue(
        priceFieldName, sizeFieldName, tradeIdFieldName
    );

    return grossTradeValue.calculateGross(inputRecordsRdd);
  }

  /*
   * The following provide metadata about the Transform used for registration
   * and display in Koverse.
   */

  /**
   * Get the name of this transform. It must not be an empty string.
   *
   * @return The name of this transform.
   */
  @Override
  public String getName() {

    return "GDAX ETH - Gross Trade Value calculator";
  }

  /**
   * Get the parameters of this transform.  The returned iterable can
   * be immutable, as it will not be altered.
   *
   * @return The parameters of this transform.
   */
  @Override
  public Iterable<Parameter> getParameters() {

    return Lists.newArrayList(
        new Parameter(PRICE_FIELD_NAME_PARAMETER, "Price Field Name", Parameter.TYPE_STRING),
        new Parameter(SIZE_FIELD_NAME_PARAMETER, "Size Field Name", Parameter.TYPE_STRING),
        new Parameter(TRADE_ID_FIELD_NAME_PARAMETER, "Trade Id Field Name", Parameter.TYPE_STRING)
    );
  }

  /**
   * Get the programmatic identifier for this transform.  It must not
   * be an empty string and must contain only alpha numeric characters.
   *
   * @return The programmatic id of this transform.
   */
  @Override
  public String getTypeId() {

    return "gdaxEthGrossTradeValue";
  }

  /**
   * Get the version of this transform.
   *
   * @return The version of this transform.
   */
  @Override
  public Version getVersion() {

    return new Version(0, 1, 0);
  }

  /**
   * Get the description of this transform.
   *
   * @return The the description of this transform.
   */
  @Override
  public String getDescription() {
    return "This calculates GDAX ETH trade information gross value";
  }
}
