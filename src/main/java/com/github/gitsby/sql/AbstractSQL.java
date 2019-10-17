package com.github.gitsby.sql;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

abstract class AbstractSQL<T> {

  private static final String AND = ") \nAND (";
  private static final String OR = ") \nOR (";

  public abstract T getSelf();

  protected abstract T createNew();

  private final Map<String, T> withMap = new LinkedHashMap<>();
  Map<String, List<Integer>> indexMap = new HashMap<>();
  Map<String, Object> valueMap = new HashMap<>();


  public T with(String view) {
    if (withMap.containsKey(view)) {
      throw new IllegalArgumentException("Already exists " + view);
    }

    {
      T t = createNew();

      withMap.put(view, t);

      return t;
    }
  }

  public T select(String columns) {
    sql().statementType = SQLStatement.StatementType.SELECT;
    sql().select.add(columns);
    return getSelf();
  }

  public T selectDistinct(String columns) {
    sql().distinct = true;
    select(columns);
    return getSelf();
  }

  public T from(String table) {
    sql().tables.add(table);
    return getSelf();
  }

  public T join(String join) {
    sql().joins.put(join, JoinType.JOIN);
    return getSelf();
  }

  public T innerJoin(String join) {
    sql().joins.put(join, JoinType.INNER);
    return getSelf();
  }

  public T leftJoin(String join) {
    sql().joins.put(join, JoinType.LEFT);
    return getSelf();
  }

  public T rightjoin(String join) {
    sql().joins.put(join, JoinType.RIGHT);
    return getSelf();
  }

  public T outerjoin(String join) {
    sql().joins.put(join, JoinType.OUTER);
    return getSelf();
  }

  public T where(String conditions) {
    sql().where.add(conditions);
    sql().lastList = sql().where;
    return getSelf();
  }

//  TODO decide need or not
//  public T or() {
//    sql().lastList.add(OR);
//    return getSelf();
//  }
//
//  public T and() {
//    sql().lastList.add(AND);
//    return getSelf();
//  }

  public T group_by(String columns) {
    sql().groupBy.add(columns);
    return getSelf();
  }

  public T having(String conditions) {
    sql().having.add(conditions);
    sql().lastList = sql().having;
    return getSelf();
  }

  public T order_by(String columns) {
    sql().orderBy.add(columns);
    return getSelf();
  }

  public T limit(String conditions) {
    sql().limit = conditions;
    return getSelf();
  }

  public T offset(String conditions) {
    sql().offset = conditions;
    return getSelf();
  }

  private SQLStatement sql = new SQLStatement();

  private SQLStatement sql() {
    return sql;
  }

  public <A extends Appendable> A usingAppender(A a) {
    sql().sql(a);
    return a;
  }

  public String compile() {
    return compileBuild((SQL) getSelf(), true);
  }

  protected String compileBuild(SQL mainSQL, boolean isMainSql) {
    StringBuilder sb = new StringBuilder();

    if (withMap.size() > 0) {
      sb.append("WITH ");

      boolean needComma = false;

      for (Entry<String, T> e : withMap.entrySet()) {
        String name = e.getKey();
        String sql;
        if ((e.getValue() instanceof SQL)) {
          SQL withSQL = ((SQL) e.getValue());
          appendValueMaps(mainSQL, withSQL);
          appendIndexMaps(mainSQL, withSQL);
          sql = withSQL.compileBuild(mainSQL, false);
        } else {
          sql = e.getKey();
        }

        if (needComma) {
          sb.append("\n, ");
        }
        needComma = true;

        sb.append(name).append(" as (");
        if (e.getValue() instanceof SQL) {
          sb.append("\n");
        }
        sb.append(sql).append(") ");
      }

      sb.append('\n');
      sb.append('\n');
    }

    sql().sql(sb);
    return parse(sb.toString(), mainSQL, isMainSql);
  }

  private void appendValueMaps(SQL mainSQL, SQL withSQL) {
    for (Entry<String, Object> entry : withSQL.valueMap.entrySet()) {
      mainSQL.valueMap.put(entry.getKey(), entry.getValue());
    }
  }

  private void appendIndexMaps(SQL mainSQL, SQL withSQL) {
    for (Entry<String, List<Integer>> entry : withSQL.indexMap.entrySet()) {
      mainSQL.indexMap.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sql().sql(sb);
    return sb.toString();
  }

  private static class SafeAppendable {

    private final Appendable a;
    private boolean empty = true;

    public SafeAppendable(Appendable a) {
      super();
      this.a = a;
    }

    public SafeAppendable append(CharSequence s) {
      try {
        if (empty && s.length() > 0) {
          empty = false;
        }
        a.append(s);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    public boolean isEmpty() {
      return empty;
    }

  }

  public enum JoinType {
    JOIN("JOIN"), INNER("INNER JOIN"), LEFT("LEFT JOIN"), RIGHT("RIGHT JOIN"), OUTER("OUTER JOIN");

    public final String sqlKeyWord;

    JoinType(String s) {
      sqlKeyWord = s;
    }
  }

  private static class SQLStatement {

    public enum StatementType {
      SELECT
    }


    HashMap<String, JoinType> joins = new LinkedHashMap<>();

    StatementType statementType;
    List<String> tables = new ArrayList<>();
    List<String> select = new ArrayList<>();
    List<String> where = new ArrayList<>();
    List<String> having = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    List<String> orderBy = new ArrayList<>();
    List<String> lastList = new ArrayList<>();
    String limit = null;
    String offset = null;
    boolean distinct;

    private void sqlClause(SafeAppendable builder, String keyword, List<String> parts, String open,
                           String close, String conjunction) {
      if (!parts.isEmpty()) {
        if (!builder.isEmpty()) {
          builder.append("\n");
        }
        builder.append(keyword);
        builder.append(" ");
        builder.append(open);
        String last = "________";
        for (int i = 0, n = parts.size(); i < n; i++) {
          String part = parts.get(i);
          if (i > 0 && !part.equals(AND) && !part.equals(OR) && !last.equals(AND)
              && !last.equals(OR)) {
            builder.append(conjunction);
          }
          builder.append(part);
          last = part;
        }
        builder.append(close);
      }
    }

    private void sqlClause(SafeAppendable builder, String keyword, String part, String open,
                           String close, String conjunction) {
      if (part != null) {
        if (!builder.isEmpty()) {
          builder.append("\n");
        }
        builder.append(keyword);
        builder.append(" ");
        builder.append(open);
        builder.append(part);
        builder.append(close);
      }
    }

    private String selectSQL(SafeAppendable builder) {
      if (distinct) {
        sqlClause(builder, "SELECT DISTINCT", select, "", "", ", \n");
      } else {
        sqlClause(builder, "SELECT", select, "", "", ", ");
      }

      sqlClause(builder, "FROM", tables, "", "", ", ");

      for (Entry<String, JoinType> joinEntry : joins.entrySet()) {
        sqlClause(builder, joinEntry.getValue().sqlKeyWord,
                  Collections.singletonList(joinEntry.getKey()), "", "",
                  joinEntry.getValue().sqlKeyWord);
      }

      sqlClause(builder, "WHERE", where, "", "", " AND ");
      sqlClause(builder, "GROUP BY", groupBy, "", "", ", ");
      sqlClause(builder, "HAVING", having, "", "", " AND ");
      sqlClause(builder, "ORDER BY", orderBy, "", "", ", ");
      sqlClause(builder, "LIMIT", limit, "", "", "");
      sqlClause(builder, "OFFSET", offset, "", "", "");
      return builder.toString();
    }

    public String sql(Appendable a) {
      SafeAppendable builder = new SafeAppendable(a);

      if (statementType == null) {
        return null;
      }

      String answer;

      switch (statementType) {

        case SELECT:
          answer = selectSQL(builder);
          break;

        default:
          answer = null;
      }

      return answer;
    }

  }

  private String parse(String query, SQL mainSQL, boolean isMainSQL) {
    int length = query.length();
    StringBuilder parsedQuery = new StringBuilder(length);
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    int index = indexMap.size() + 1;

    for (int i = 0; i < length; i++) {
      char c = query.charAt(i);
      if (inSingleQuote) {
        if (c == '\'') {
          inSingleQuote = false;
        }
      } else if (inDoubleQuote) {
        if (c == '"') {
          inDoubleQuote = false;
        }
      } else {
        if (c == '\'') {
          inSingleQuote = true;
        } else if (c == '"') {
          inDoubleQuote = true;
        } else if (c == ':' && i + 1 < length && query.charAt(i - 1) != ':' &&
            Character.isJavaIdentifierStart(query.charAt(i + 1))) {
          int j = i + 2;
          while (j < length && Character.isJavaIdentifierPart(query.charAt(j))) {
            j++;
          }
          String name = query.substring(i + 1, j);
          c = '?';
          i += name.length();

          List<Integer> indexList = indexMap.computeIfAbsent(name, k -> new LinkedList<>());
          indexList.add(index);
          if (!isMainSQL) {
            int mainIndex = mainSQL.indexMap.size() + 1;
            List<Integer> mainIndexList = mainSQL.indexMap
                .computeIfAbsent(name, k -> new LinkedList<>());
            mainIndexList.add(mainIndex);
          }

          index++;
        }
      }
      parsedQuery.append(c);
    }
    for (Entry<String, List<Integer>> entry : indexMap.entrySet()) {
      List list = entry.getValue();
      int[] indexes = new int[list.size()];
      int i = 0;
      for (Object aList : list) {
        Integer x = (Integer) aList;
        indexes[i++] = x;
      }
      entry.setValue(Arrays.stream(indexes).boxed().collect(Collectors.toList()));
    }

    return parsedQuery.toString();
  }

  private List<Integer> getIndexes(String name) {
    List<Integer> indexes = indexMap.get(name);
    if (indexes == null) {
      throw new IllegalArgumentException("Parameter not found: " + name);
    }
    return indexes;
  }

  private void setObject(String name, Object value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      ps.setObject(index, value);
    }
  }


  private void setString(String name, String value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      ps.setString(index, value);
    }
  }


  private void setInt(String name, int value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      ps.setInt(index, value);
    }
  }


  private void setLong(String name, long value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      ps.setLong(index, value);
    }
  }

  private void setTimestamp(String name, Timestamp value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      ps.setTimestamp(index, value);
    }
  }

  private void setDate(String name, Date value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      if (value instanceof java.sql.Date) {
        ps.setDate(index, (java.sql.Date) value);
      } else {
        ps.setTimestamp(index, new Timestamp(value.getTime()));
      }
    }
  }

  private void setEnum(String name, Enum value, PreparedStatement ps) throws SQLException {
    List<Integer> indexes = getIndexes(name);
    for (Integer index : indexes) {
      ps.setString(index, value.name());
    }
  }

  public T setValue(String name, Object value) {
    valueMap.put(name, value);
    return getSelf();
  }

  public PreparedStatement applyParameter(PreparedStatement ps) throws SQLException {
    for (Entry<String, Object> entry : valueMap.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      if (value instanceof Integer) {
        setInt(key, (Integer) value, ps);
      } else if (value instanceof String) {
        setString(key, (String) value, ps);
      } else if (value instanceof Timestamp) {
        setTimestamp(key, (Timestamp) value, ps);
      } else if (value instanceof Long) {
        setLong(key, (Long) value, ps);
      } else if (value instanceof java.sql.Date) {
        setDate(key, (java.sql.Date) value, ps);
      } else if (value instanceof Date) {
        setDate(key, (Date) value, ps);
      } else if (value instanceof Enum) {
        setEnum(key, (Enum) value, ps);
      } else {
        setObject(key, value, ps);
      }
    }
    return ps;
  }

}
