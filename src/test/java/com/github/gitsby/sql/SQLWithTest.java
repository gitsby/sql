package com.github.gitsby.sql;


import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class SQLWithTest {

  @Test
  public void inner_join() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table x")
        .innerJoin("test_table2 x1 on x1.col = x.col");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table as (\nselect 1\nfrom test_table x\ninner join test_table2 x1 on x1.col = x.col\n)\nselect 1"
    );
  }

  @Test
  public void left_join() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table x")
        .leftJoin("test_table2 x1 on x1.col = x.col");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table as (\nselect 1\nfrom test_table x\nleft join test_table2 x1 on x1.col = x.col\n)\nselect 1"
    );
  }

  @Test
  public void right_join() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table x")
        .rightjoin("test_table2 x1 on x1.col = x.col");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table as (\nselect 1\nfrom test_table x\nright join test_table2 x1 on x1.col = x.col\n)\nselect 1"
    );
  }

  @Test
  public void outer_join() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table x")
        .outerjoin("test_table2 x1 on x1.col = x.col");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table as (\nselect 1\nfrom test_table x\nouter join test_table2 x1 on x1.col = x.col\n)\nselect 1");
  }

  @Test
  public void join_order() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table x")
        .outerjoin("test_table2 x1 on x1.col = x.col")
        .leftJoin("test_table3 x2 on x2.col = x.col")
        .innerJoin("test_table4 x3 on x3.col = x.col");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as (\n"
                                                   + "select 1\nfrom test_table x"
                                                   + "\nouter join test_table2 x1 on x1.col = x.col"
                                                   + "\nleft join test_table3 x2 on x2.col = x.col"
                                                   + "\ninner join test_table4 x3 on x3.col = x.col"
                                                   + "\n)"
                                                   + "\nselect 1");
  }

  @Test
  public void orderBy() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table")
        .order_by("1");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery)
        .isEqualToIgnoringCase("with with_table as (\nselect 1\nfrom test_table\norder by 1\n)\nselect 1");
  }

  @Test
  public void group_by() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table")
        .group_by("asd");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery)
        .isEqualToIgnoringCase("with with_table as (\nselect 1\nfrom test_table\ngroup by asd\n)\nselect 1");
  }

  @Test
  public void group_by_having() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table")
        .group_by("asd")
        .having("asd > 0");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table as (\nselect 1\nfrom test_table\ngroup by asd\nhaving asd > 0\n)\nselect 1"
    );
  }


  @Test
  public void limit_offset() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table")
        .limit("1")
        .offset("5");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery)
        .isEqualToIgnoringCase("with with_table as (\nselect 1\nfrom test_table\nlimit 1\noffset 5\n)\nselect 1");
  }

  @Test
  public void limit() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table")
        .limit("1");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as (\nselect 1\nfrom test_table\nlimit 1\n)\nselect 1");
  }


  @Test
  public void from() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as (\nselect 1\nfrom test_table\n)\nselect 1");
  }

  @Test
  public void select() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1!");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as (\nselect 1!\n)\nselect 1");
  }

  @Test
  public void select_distinct() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .selectDistinct("1!");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as (\nselect distinct 1!\n)\nselect 1");
  }

  @Test
  public void select_multiple_columns() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1!")
        .select("2")
        .select("3");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as (\nselect 1!, 2, 3\n)\nselect 1");
  }

  @Test
  public void replace_namedParameter_to_placeholder() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .from("test_table")
        .where("column1 = :param1")
        .where("column2 = :param2")
        .where("column3 = :param2")
        .setValue("param1", "someValue1")
        .setValue("param2", "someValue2");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase("with with_table as ("
                                                   + "\nselect 1"
                                                   + "\nfrom test_table"
                                                   + "\nwhere column1 = ?"
                                                   + " and column2 = ?"
                                                   + " and column3 = ?"
                                                   + "\n)"
                                                   + "\nselect 1");
  }

  @Test
  public void fill_indexMap_and_valueMap_for_setValue() {
    SQL sql = new SQL()
        .select("1");

    SQL withSql = sql.with("with_table");
    withSql
        .select("1")
        .from("test_table")
        .where("column1 = :param1")
        .where("column2 = :param2")
        .where("column3 = :param1")
        .setValue("param1", "someValue1")
        .setValue("param2", "someValue2");

    sql.compile();

    assertThat(withSql.indexMap).isNotNull();
    assertThat(withSql.indexMap.size()).isEqualTo(2);

    assertThat(withSql.indexMap.get("param1")).isNotNull();
    assertThat(withSql.indexMap.get("param1")).hasSize(2);
    assertThat(withSql.indexMap.get("param1").get(0)).isEqualTo(1);
    assertThat(withSql.indexMap.get("param1").get(1)).isEqualTo(3);

    assertThat(withSql.indexMap.get("param2")).isNotNull();
    assertThat(withSql.indexMap.get("param2")).hasSize(1);
    assertThat(withSql.indexMap.get("param2").get(0)).isEqualTo(2);

    assertThat(withSql.valueMap).isNotNull();
    assertThat(withSql.valueMap.size()).isEqualTo(2);
    assertThat(withSql.valueMap.get("param1")).isEqualTo("someValue1");
    assertThat(withSql.valueMap.get("param2")).isEqualTo("someValue2");
  }


  @Test
  public void where() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .select("2")
        .from("test_table")
        .where("test_table.column1 = 'asd'");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery)
        .isEqualToIgnoringCase(
            "with with_table as (\nselect 1, 2\nfrom test_table\nwhere test_table.column1 = 'asd'\n)\nselect 1"
        );
  }

  @Test
  public void where_multiple() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table")
        .select("1")
        .select("2")
        .from("test_table")
        .where("test_table.column1 = 'asd'")
        .where("test_table.column2 >= 42");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table as (\nselect 1, 2\nfrom test_table\nwhere test_table.column1 = 'asd' and test_table.column2 >= 42\n)\nselect 1"
    );
  }

  @Test
  public void with_multiple() {
    SQL sql = new SQL()
        .select("1");

    sql.with("with_table1")
        .select("1")
        .from("test_table");

    sql.with("with_table2")
        .select("2")
        .from("test_table");

    sql.with("with_table3")
        .select("3")
        .from("test_table");

    String sqlQuery = sql.compile();

    assertThat(sqlQuery).isEqualToIgnoringCase(
        "with with_table1 as (\nselect 1\nfrom test_table\n)"
            + "\n, with_table2 as (\nselect 2\nfrom test_table\n)"
            + "\n, with_table3 as (\nselect 3\nfrom test_table\n)"
            + "\nselect 1"
    );
  }


  @Test
  public void with_multiple_fill_indexMap_and_valueMap_for_setValue() {
    SQL sql = new SQL();

    SQL withSql1 = sql.with("with_table1");
    withSql1
        .select("1")
        .from("test_table")
        .where("column1 = :param1 and column2 = :param2")
        .setValue("param1", "someValue1");

    SQL withSql2 = sql.with("with_table2");
    withSql2
        .select("1")
        .from("test_table")
        .where("column1 = :param1 and column2 = :param2 and column3 = :param3")
        .setValue("param2", "someValue2");

    SQL withSql3 = sql.with("with_table3");
    withSql3
        .select("1")
        .from("test_table")
        .where("column1 = :param1 and column2 = :param2 and column3 = :param3");

    sql.select("1")
        .where("column3 = :param3 and column1 = :param1")
        .setValue("param3", "someValue3");

    sql.compile();

    assertThat(sql.indexMap).isNotNull();
    assertThat(sql.indexMap.size()).isEqualTo(3);

    assertThat(sql.indexMap.get("param1")).isNotNull();
    assertThat(sql.indexMap.get("param1")).hasSize(4);
    assertThat(sql.indexMap.get("param1").get(0)).isEqualTo(1);
    assertThat(sql.indexMap.get("param1").get(1)).isEqualTo(3);
    assertThat(sql.indexMap.get("param1").get(2)).isEqualTo(6);
    assertThat(sql.indexMap.get("param1").get(3)).isEqualTo(10);

    assertThat(sql.indexMap.get("param2")).isNotNull();
    assertThat(sql.indexMap.get("param2")).hasSize(3);
    assertThat(sql.indexMap.get("param2").get(0)).isEqualTo(2);
    assertThat(sql.indexMap.get("param2").get(1)).isEqualTo(4);
    assertThat(sql.indexMap.get("param2").get(2)).isEqualTo(7);

    assertThat(sql.indexMap.get("param3")).isNotNull();
    assertThat(sql.indexMap.get("param3")).hasSize(3);
    assertThat(sql.indexMap.get("param3").get(0)).isEqualTo(5);
    assertThat(sql.indexMap.get("param3").get(1)).isEqualTo(8);
    assertThat(sql.indexMap.get("param3").get(2)).isEqualTo(9);

    assertThat(sql.valueMap).isNotNull();
    assertThat(sql.valueMap.size()).isEqualTo(3);
    assertThat(sql.valueMap.get("param1")).isEqualTo("someValue1");
    assertThat(sql.valueMap.get("param2")).isEqualTo("someValue2");
    assertThat(sql.valueMap.get("param3")).isEqualTo("someValue3");
  }

}
