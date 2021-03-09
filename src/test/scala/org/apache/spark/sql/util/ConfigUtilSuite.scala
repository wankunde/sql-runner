// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import org.scalatest.{FunSuite, Matchers}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-17.
 */
class ConfigUtilSuite extends FunSuite with Matchers {


  test("trim config array") {

    val columnName = "\n            日期,店铺id,店铺名,买家付款\n          "
    val dbColumnName = "\n            {dt},{store_id},{store_name},{buyer_payment}," +
      "{buyer_prepaid}," +
      "{inquiry_tailing},{no_order_try},\n            {size_query_succeeded},{applicable_season}," +
      "{enable_filter_applicable_season},{chat_expires_at},{r2_expires_at},{audit_expires_at}\n  " +
      "  ";
    ConfigUtil.trimConfigValue(columnName) should be("日期,店铺id,店铺名,买家付款")

    ConfigUtil.trimConfigArray(dbColumnName, ",") should be(
      "{dt},{store_id},{store_name},{buyer_payment},{buyer_prepaid},{inquiry_tailing}," +
        "{no_order_try},{size_query_succeeded},{applicable_season}," +
        "{enable_filter_applicable_season},{chat_expires_at},{r2_expires_at},{audit_expires_at}")

  }

}
