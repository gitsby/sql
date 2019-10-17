package com.github.gitsby.sql;


import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class SQLSelectTest {

  @Test
  public void from() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    String sqlQuery = sql.toString();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1\nfrom test_table");
  }

  @Test
  public void select() {
    SQL sql = new SQL();
    sql.select("1!");
    String sqlQuery = sql.toString();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1!");
  }


  @Test
  public void select_multiple_columns() {
    SQL sql = new SQL();
    sql.select("1!");
    sql.select("2");
    sql.select("3");
    String sqlQuery = sql.toString();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1!, 2, 3");
  }


  @Test
  public void where() {
    SQL sql = new SQL();
    sql.select("1");
    sql.select("2");
    sql.from("test_table");
    sql.where("test_table.column1 = 'asd'");
    String sqlQuery = sql.toString();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1, 2\nfrom test_table\nwhere ( test_table.column1 = 'asd' )");
  }

  @Test
  public void where_multiple() {
    SQL sql = new SQL();
    sql.select("1");
    sql.select("2");
    sql.from("test_table");
    sql.where("test_table.column1 = 'asd'");
    sql.where("test_table.column2 >= 42");
    String sqlQuery = sql.toString();
    assertThat(sqlQuery).isEqualToIgnoringCase("select 1, 2\nfrom test_table\nwhere test_table.column1 = 'asd' and test_table.column2 >= 42 ");
  }


}
