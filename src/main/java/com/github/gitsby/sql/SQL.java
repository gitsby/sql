package com.github.gitsby.sql;

public class SQL extends AbstractSQL<SQL> {

  @Override
  public SQL getSelf() {
    return this;
  }

  @Override
  protected SQL createNew() {
    return new SQL();
  }

}
