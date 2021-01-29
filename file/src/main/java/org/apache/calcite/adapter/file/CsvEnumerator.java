/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;

import org.apache.commons.lang3.time.FastDateFormat;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Enumerator that reads from a CSV file.
 * 从 CSV 文件读取数据的枚举器。
 *
 * @param <E> Row type 行类型
 */
public class CsvEnumerator<E> implements Enumerator<E> {
  private final CSVReader reader;
  private final List<String> filterValues;
  private final AtomicBoolean cancelFlag;
  private final RowConverter<E> rowConverter;
  private E current;

  private static final FastDateFormat TIME_FORMAT_DATE;
  private static final FastDateFormat TIME_FORMAT_TIME;
  private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

  static {
    final TimeZone gmt = TimeZone.getTimeZone("GMT");
    TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
    TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
    TIME_FORMAT_TIMESTAMP =
        FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
      List<CsvFieldType> fieldTypes, List<Integer> fields) {
    //noinspection unchecked
    this(source, cancelFlag, false, null,
        (RowConverter<E>) converter(fieldTypes, fields));
  }

  /**
   * @param source 数据资源
   * @param cancelFlag 用户是否取消了对查询的请求
   * @param stream 数据源是否是流
   * @param filterValues 过滤的数据：Scan表没有过滤
   * @param rowConverter
   */
  public CsvEnumerator(Source source,
                      AtomicBoolean cancelFlag,
                      boolean stream,
                      // todo 怎么根据filterValues过滤数据的
                      String[] filterValues,
                      RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    // 获取 过滤列表 的不可变视图
    this.filterValues = filterValues == null ? null
        : ImmutableNullableList.copyOf(filterValues);
    try {
      // 如果是流，则使用CsvStreamReader，
      if (stream) {
        this.reader = new CsvStreamReader(source);
      } else {
        this.reader = openCsv(source);
      }

      // skip header row
      // 跳过对表头的读取
      this.reader.readNext();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RowConverter<?> converter(List<CsvFieldType> fieldTypes,
      List<Integer> fields) {
    if (fields.size() == 1) {
      final int field = fields.get(0);
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return arrayConverter(fieldTypes, fields, false);
    }
  }

  /**
   * @param fieldTypes
   * @param fields
   * @param stream
   * @return
   */
  public static RowConverter<Object[]> arrayConverter(List<CsvFieldType> fieldTypes, List<Integer> fields, boolean stream) {
    return new ArrayRowConverter(fieldTypes, fields, stream);
  }

  /**
   * Deduces the names and types of a table's columns
   * by reading the first line of a CSV file.
   *
   * kp 通过读取csv文件第一样，推断每一列的名称和类型
   */
  static RelDataType deduceRowType(JavaTypeFactory typeFactory, Source source,
      List<CsvFieldType> fieldTypes) {
    return deduceRowType(typeFactory, source, fieldTypes, false);
  }

  /**
   * Deduces(推断) the names and types of a table's columns by reading the first line of a CSV file.
   * kp 通过读取csv文件第一行，推断 table 的类型信息
   *
   * @param typeFactory
   * @param source
   * @param fieldTypes kp 存放结果
   * @param stream
   * @return
   */
  public static RelDataType deduceRowType(JavaTypeFactory typeFactory,
                                          Source source,
                                          List<CsvFieldType> fieldTypes,
                                          Boolean stream) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    if (stream) {
      names.add(FileSchemaFactory.ROWTIME_COLUMN_NAME);
      types.add(typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
    }
    try (CSVReader reader = openCsv(source)) {
      String[] strings = reader.readNext();
      if (strings == null) {
        strings = new String[]{"EmptyFileHasNoColumns:boolean"};
      }
      for (String string : strings) {
        final String name;
        final CsvFieldType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          fieldType = CsvFieldType.of(typeString);
          if (fieldType == null) {
            System.out.println("WARNING: Found unknown type: "
                + typeString + " in file: " + source.path()
                + " for column: " + name
                + ". Will assume the type of column is string");
          }
        } else {
          name = string;
          fieldType = null;
        }
        final RelDataType type;
        if (fieldType == null) {
          type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else {
          type = fieldType.toType(typeFactory);
        }
        names.add(name);
        types.add(type);
        if (fieldTypes != null) {
          fieldTypes.add(fieldType);
        }
      }
    } catch (IOException e) {
      // ignore
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  static CSVReader openCsv(Source source) throws IOException {
    Objects.requireNonNull(source, "source");
    return new CSVReader(source.reader());
  }

  @Override public E current() {
    return current;
  }

  // 移动游标到下一个节点，返回false表示已经到达尾部
  @Override
  public boolean moveNext() {
    try {
      outer:
      for (; ; ) {
        // 如果用户执行了取消请求，则返回false。
        if (cancelFlag.get()) {
          return false;
        }

        // 读取下一行数据，数据是使用逗号分割的
        final String[] strings = reader.readNext();
        if (strings == null) {
          // 如果是流文件，则等待，然后重试
          // 否则关闭reader、并返回false。
          if (reader instanceof CsvStreamReader) {
            // 休眠2000毫秒，在继续读取
            try {
              Thread.sleep(CsvStreamReader.DEFAULT_MONITOR_DELAY);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            continue;
          }
          current = null;
          reader.close();
          return false;
        } // end of "strings is null"

        // 读取的数据不为空
        // 检查是否有需要过滤的数据
        if (filterValues != null) {
          for (int i = 0; i < strings.length; i++) {
            String filterValue = filterValues.get(i);
            if (filterValue != null) {
              // 如果需要过滤的数据和当前遍历的数据不相同
              if (!filterValue.equals(strings[i])) {
                // label 是对于嵌套的循环、可以直接跳出内循环、在外循环继续。
                /// kp 继续开始外层循环，内层循环之后 tag_1 处的代码不会执行。
                continue outer;
              }
            }
          }
        }
        // tag 1: 指针指向当前行数据
        //        rowConverter包含字段的类型信息
        current = rowConverter.convertRow(strings);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing CSV reader", e);
    }
  }

  // todo 改成 IntStream
  /** Returns an array of integers {0, ..., n - 1}. */
  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  /** Row converter.
   *
   * @param <E> element type */
  abstract static class RowConverter<E> {

    abstract E convertRow(String[] rows);

    // 将 value 转换为 fieldType 类型的数据
    @SuppressWarnings("JdkObsolete")
    protected Object convert(CsvFieldType fieldType, String value) {
      if (fieldType == null) {
        return value;
      }
      switch (fieldType) {
      case BOOLEAN:
        if (value.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(value);
      case BYTE:
        if (value.length() == 0) {
          return null;
        }
        return Byte.parseByte(value);
      case SHORT:
        if (value.length() == 0) {
          return null;
        }
        return Short.parseShort(value);
      case INT:
        if (value.length() == 0) {
          return null;
        }
        return Integer.parseInt(value);
      case LONG:
        if (value.length() == 0) {
          return null;
        }
        return Long.parseLong(value);
      case FLOAT:
        if (value.length() == 0) {
          return null;
        }
        return Float.parseFloat(value);
      case DOUBLE:
        if (value.length() == 0) {
          return null;
        }
        return Double.parseDouble(value);
      case DATE:
        if (value.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_DATE.parse(value);
          return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
        } catch (ParseException e) {
          return null;
        }
      case TIME:
        if (value.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIME.parse(value);
          return (int) date.getTime();
        } catch (ParseException e) {
          return null;
        }
      case TIMESTAMP:
        if (value.length() == 0) {
          return null;
        }
        try {
          Date date = TIME_FORMAT_TIMESTAMP.parse(value);
          return date.getTime();
        } catch (ParseException e) {
          return null;
        }
      case STRING:
      default:
        return value;
      }
    }
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    /** Field types. List must not be null, but any element may be null. */
    private final List<CsvFieldType> fieldTypes;
    private final ImmutableIntList fields;
    /** Whether the row to convert is from a stream. */
    private final boolean stream;

    ArrayRowConverter(List<CsvFieldType> fieldTypes, List<Integer> fields,
        boolean stream) {
      this.fieldTypes = ImmutableNullableList.copyOf(fieldTypes);
      this.fields = ImmutableIntList.copyOf(fields);
      this.stream = stream;
    }

    @Override public Object[] convertRow(String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public Object[] convertNormalRow(String[] strings) {
      final Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        objects[i] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }

    public Object[] convertStreamRow(String[] strings) {
      final Object[] objects = new Object[fields.size() + 1];
      objects[0] = System.currentTimeMillis();
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        objects[i + 1] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter<Object> {
    private final CsvFieldType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(CsvFieldType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    @Override public Object convertRow(String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}
