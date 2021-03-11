// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import org.scalatest.{FunSuite, Matchers}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-24.
 */
class CommandSuite extends FunSuite with Matchers {

  val textHeader =
    s"""/************************************************
       |
       |  author: kun.wan
       |  period: day
       |  run_env: PRD & PRE
       |  describe: policy_store_config 店铺数据量检查
       |  app.id: 303
       |
       |************************************************/
       |""".stripMargin

  test("test parse job text") {
    val text =
      s"""$textHeader
         |-- 测试一下单行注释
         |
         |!set mykey=myvalue;
         |!set longKey = \"(
         |select *
         |from tab
         |WHERE dates = '{date | yyyy - MM - dd}'
         |) as q\";
         |
         |SELECT id, name
         |FROM test_db.test_name
         |WHERE id in ('001', '002');
         |
         |-- 测试SQL中包含引号
         |SELECT 'a;b' as a, "abc;hhh" as b,'a\\'b' as c;
         |""".stripMargin

    val commands = CommandFactory.parseCommands(text)

    commands.length should be(7)
  }

  test("test parse if command") {
    Seq("kun.wan", "King").map { username =>
      val text =
        s"""$textHeader
           |!set user = $username;
           |!if (user = 'kun.wan')
           |  select 'if command';
           |!else
           |  select 'else command';
           |!fi
           |""".stripMargin

      val commands = CommandFactory.parseCommands(text)

      commands.length should be(3)

      commands.foreach(_.run())
    }

    val text =
      s"""$textHeader
         |
         |SELECT /*+ COLLECT_VALUE('row_count', 'c') */ count(1) as c;
         |SELECT /*+ COLLECT_VALUE('row_count2', 'd') */ count(1) as d;
         |
         |!if (row_count = row_count2 and row_count = 1)
         |  select 'row count is 1';
         |!else
         |  select 'row count is not 1';
         |!fi
         |""".stripMargin

    val commands = CommandFactory.parseCommands(text)

    commands.length should be(4)

    commands.foreach(_.run())
  }

}
