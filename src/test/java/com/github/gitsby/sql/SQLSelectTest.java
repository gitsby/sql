package com.github.gitsby.sql;


import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class SQLSelectTest {

  @Test
  public void inner_join() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table x");
    sql.innerJoin("test_table2 x1 on x1.col = x.col");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase(
        "select 1\nfrom test_table x\ninner join test_table2 x1 on x1.col = x.col");
  }

  @Test
  public void left_join() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table x");
    sql.leftJoin("test_table2 x1 on x1.col = x.col");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase(
        "select 1\nfrom test_table x\nleft join test_table2 x1 on x1.col = x.col");
  }

  @Test
  public void right_join() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table x");
    sql.rightjoin("test_table2 x1 on x1.col = x.col");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase(
        "select 1\nfrom test_table x\nright join test_table2 x1 on x1.col = x.col");
  }

  @Test
  public void outer_join() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table x");
    sql.outerjoin("test_table2 x1 on x1.col = x.col");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase(
        "select 1\nfrom test_table x\nouter join test_table2 x1 on x1.col = x.col");
  }

  @Test
  public void join_order() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table x");
    sql.outerjoin("test_table2 x1 on x1.col = x.col");
    sql.leftJoin("test_table3 x2 on x2.col = x.col");
    sql.innerJoin("test_table4 x3 on x3.col = x.col");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table x"
                                                   + "\nouter join test_table2 x1 on x1.col = x.col"
                                                   + "\nleft join test_table3 x2 on x2.col = x.col"
                                                   + "\ninner join test_table4 x3 on x3.col = x.col");
  }

  @Test
  public void orderBy() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    sql.order_by("1");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table\norder by 1");
  }

  @Test
  public void group_by() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    sql.group_by("asd");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table\ngroup by asd");
  }

  @Test
  public void group_by_having() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    sql.group_by("asd");
    sql.having("asd > 0");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery)
        .isEqualToIgnoringCase("select 1\nfrom test_table\ngroup by asd\nhaving asd > 0");
  }


  @Test
  public void limit_offset() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    sql.limit("1");
    sql.offset("5");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table\nlimit 1\noffset 5");
  }

  @Test
  public void limit() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    sql.limit("1");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table\nlimit 1");
  }


  @Test
  public void from() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table");
  }

  @Test
  public void select() {
    SQL sql = new SQL();
    sql.select("1!");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1!");
  }

  @Test
  public void select_distinct() {
    SQL sql = new SQL();
    sql.selectDistinct("1!");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select distinct 1!");
  }

  @Test
  public void select_multiple_columns() {
    SQL sql = new SQL();
    sql.select("1!");
    sql.select("2");
    sql.select("3");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1!, 2, 3");
  }

  @Test
  public void replace_namedParameter_to_placeholder() {
    SQL sql = new SQL();
    sql.select("1");
    sql.select("2");
    sql.from("test_table");
    sql.where("test_table.column1 = :param");
    sql.setValue("param", "asd");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery)
        .isEqualToIgnoringCase("select 1, 2\nfrom test_table\nwhere test_table.column1 = ?");
  }


  @Test
  public void where() {
    SQL sql = new SQL();
    sql.select("1");
    sql.select("2");
    sql.from("test_table");
    sql.where("test_table.column1 = 'asd'");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery)
        .isEqualToIgnoringCase("select 1, 2\nfrom test_table\nwhere test_table.column1 = 'asd'");
  }

  @Test
  public void where_multiple() {
    SQL sql = new SQL();
    sql.select("1");
    sql.select("2");
    sql.from("test_table");
    sql.where("test_table.column1 = 'asd'");
    sql.where("test_table.column2 >= 42");
    String sqlQuery = sql.compile();
    assertThat(sqlQuery).isEqualToIgnoringCase(
        "select 1, 2\nfrom test_table\nwhere test_table.column1 = 'asd' and test_table.column2 >= 42");
  }


}
