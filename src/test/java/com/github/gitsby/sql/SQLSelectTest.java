package com.github.gitsby.sql;


import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class SQLSelectTest {

  @Test
  public void select_from() {
    SQL sql = new SQL();
    sql.select("1");
    sql.from("test_table");
    String s = sql.toString();
    assertThat(s).isEqualToIgnoringCase("select 1\nfrom test_table");
  }

}
