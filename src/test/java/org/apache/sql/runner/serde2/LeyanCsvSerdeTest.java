package org.apache.sql.runner.serde2;

import org.junit.Test;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-04-21.
 */
public class LeyanCsvSerdeTest {

  @Test
  public void testGetInspector() {
    LeyanCsvSerde leyanCsvSerde = new LeyanCsvSerde();
    String columnTypes="string:int:string:string:string:bigint:bigint:bigint:string:string:bigint:boolean:string:array<string>:struct<task_id:bigint,round_id:bigint,round:int>";
//    leyanCsvSerde.getInspector(columnTypes);
  }

}
