package org.apache.sql.runner.serde2;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * OpenCSVSerde use opencsv to deserialize CSV format.
 * Users can specify custom separator, quote or escape characters. And the default separator(\),
 * quote("), and escape characters(\) are the same as the opencsv library.
 *
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS,
    org.apache.hadoop.hive.serde2.OpenCSVSerde.SEPARATORCHAR,
    org.apache.hadoop.hive.serde2.OpenCSVSerde.QUOTECHAR,
    org.apache.hadoop.hive.serde2.OpenCSVSerde.ESCAPECHAR})
public final class LeyanCsvSerde extends AbstractSerDe {

  private final static Logger LOG = LoggerFactory.getLogger(LeyanCsvSerde.class);
  TypeInfo rowTypeInfo;

  private ObjectInspector rowOI;
  int numCols;
  List<TypeInfo> columnTypes;
  List<Object> row;

  private String[] outputFields;

  private char separatorChar;
  private char quoteChar;
  private char escapeChar;

  public static final String SEPARATORCHAR = "separatorChar";
  public static final String QUOTECHAR = "quoteChar";
  public static final String ESCAPECHAR = "escapeChar";

  @SuppressWarnings("deprecation")
  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

    final List<String> columnNames = Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS)
        .split(","));
    numCols = columnNames.size();
    columnTypes =
        TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES));

    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

    rowOI = getStandardObjectInspectorFromTypeInfo(rowTypeInfo);

    separatorChar = getProperty(tbl, SEPARATORCHAR, CSVWriter.DEFAULT_SEPARATOR);
    quoteChar = getProperty(tbl, QUOTECHAR, CSVWriter.DEFAULT_QUOTE_CHARACTER);
    escapeChar = getProperty(tbl, ESCAPECHAR, CSVWriter.DEFAULT_ESCAPE_CHARACTER);

    row = new ArrayList<>(columnNames.size());
    for (int i = 0; i < numCols; i++) {
      row.add(null);
    }

    outputFields = new String[numCols];
  }

  static HashMap<TypeInfo, ObjectInspector> cachedObjectInspectors =
      new HashMap<TypeInfo, ObjectInspector>();

  public static ObjectInspector getStandardObjectInspectorFromTypeInfo(TypeInfo typeInfo) {
    ObjectInspector oi = cachedObjectInspectors.get(typeInfo);
    if (oi == null) {
      LOG.debug("Got asked for OI for {}, [{}]", typeInfo.getCategory(), typeInfo.getTypeName());
      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
              ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
          break;
        case STRUCT:
          StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
          List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
          List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
          List<ObjectInspector> fieldObjectInspectors =
              new ArrayList<ObjectInspector>(fieldTypeInfos.size());
          for (int i = 0; i < fieldTypeInfos.size(); i++) {
            fieldObjectInspectors
                .add(getStandardObjectInspectorFromTypeInfo(fieldTypeInfos.get(i)));
          }
          oi = ObjectInspectorFactory.getStandardStructObjectInspector(
              fieldNames, fieldObjectInspectors
          );
          break;
        case LIST:
          ObjectInspector elementObjectInspector = getStandardObjectInspectorFromTypeInfo(
              ((ListTypeInfo) typeInfo).getListElementTypeInfo());
          oi = ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
          break;
        case MAP:
          ObjectInspector keyObjectInspector = getStandardObjectInspectorFromTypeInfo(
              ((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
          ObjectInspector valueObjectInspector = getStandardObjectInspectorFromTypeInfo(
              ((MapTypeInfo) typeInfo).getMapValueTypeInfo());
          oi = ObjectInspectorFactory
              .getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
          break;
        default:
          oi = null;
      }
      cachedObjectInspectors.put(typeInfo, oi);
    }
    return oi;
  }


  private char getProperty(final Properties tbl, final String property, final char def) {
    final String val = tbl.getProperty(property);

    if (val != null) {
      return val.charAt(0);
    }

    return def;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
    final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();

    if (outputFieldRefs.size() != numCols) {
      throw new SerDeException("Cannot serialize the object because there are "
          + outputFieldRefs.size() + " fields but the table has " + numCols + " columns.");
    }

    try {
      // Get all data out.
      for (int c = 0; c < numCols; c++) {
        final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
        outputFields[c] = fieldStringValue(columnTypes.get(c), field);
      }

      final StringWriter writer = new StringWriter();
      final CSVWriter csv = newWriter(writer, separatorChar, quoteChar, escapeChar);

      csv.writeNext(outputFields);
      csv.close();
      return new Text(writer.toString());
    } catch (final IOException ioe) {
      throw new SerDeException(ioe);
    }
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    Text rowText = (Text) blob;

    CSVReader csv = null;
    String[] read = null;
    try {
      csv = newReader(new CharArrayReader(rowText.toString().toCharArray()), separatorChar,
          quoteChar, escapeChar);
      read = csv.readNext();

      for (int i = 0; i < numCols; i++) {
        if (read != null && i < read.length) {
          Object fieldValue = parseFieldValue(columnTypes.get(i), read[i]);
          row.set(i, fieldValue);
        } else {
          row.set(i, null);
        }
      }

      return row;
    } catch (final Exception e) {
      String rawRow = (read != null) ? Arrays.toString(read) : "";
      throw new SerDeException("rawRow : "+ rawRow,e);
    } finally {
      if (csv != null) {
        try {
          csv.close();
        } catch (final Exception e) {
          LOG.error("fail to close csv writer ", e);
        }
      }
    }
  }

  private static ObjectMapper mapper = new ObjectMapper();

  private String fieldStringValue(TypeInfo typeInfo, Object field) throws IOException {
    if(field==null)
      return "NULL";

    if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return Double.toString((Double) field);
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return Boolean.toString((Boolean) field);
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return Float.toString((Float) field);
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return Integer.toString((Integer) field);
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return Long.toString((Long) field);
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return (String) field;
    } else if (typeInfo instanceof DecimalTypeInfo) {
      return field.toString();
    } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
      Map<String,Object> map = new TreeMap<>();
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<String> structFieldNames = structTypeInfo.getAllStructFieldNames();
      for (int i = 0; i < structFieldNames.size(); i++) {
        String fieldName = structFieldNames.get(i);
        map.put(fieldName, ((List)field).get(i));
      }

      return mapper.writeValueAsString(map);
    } else if (typeInfo.getCategory().equals(Category.LIST)) {
      return mapper.writeValueAsString(field);
    } else if (typeInfo.getCategory().equals(Category.MAP)) {
      return mapper.writeValueAsString(field);
    } else if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
      return Byte.toString((Byte) field);
    } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
      return Short.toString((Short) field);
    } else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
      return field.toString();
    } else {
      throw new UnsupportedOperationException("Unknown stringValue type: " + typeInfo);
    }
  }

  @SuppressWarnings("unchecked")
  private Object parseFieldValue(TypeInfo typeInfo, String stringValue)
      throws UnsupportedOperationException, IOException {
    if ("NULL".equals(stringValue)) {
      return null;
    }

    if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return Double.parseDouble(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return Boolean.parseBoolean(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return Float.parseFloat(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return Integer.parseInt(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return Long.parseLong(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return stringValue;
    } else if (typeInfo instanceof DecimalTypeInfo) {
      return HiveDecimal.create(stringValue);
    } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
      Map map = mapper.readValue(stringValue, Map.class);

      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<String> structFieldNames = structTypeInfo.getAllStructFieldNames();
      Object[] structFieldValues = new Object[structFieldNames.size()];
      for (int i = 0; i < structFieldNames.size(); i++) {
        String fieldName = structFieldNames.get(i);
        Object structFieldValue = map.getOrDefault(fieldName, "NULL");
        TypeInfo structFieldTypeInfo = structTypeInfo.getStructFieldTypeInfo(fieldName);
        structFieldValues[i] = parseFieldValue(structFieldTypeInfo, structFieldValue.toString());
      }
      return Arrays.asList(structFieldValues);
    } else if (typeInfo.getCategory().equals(Category.LIST)) {
      List list = mapper.readValue(stringValue, List.class);

      ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
      TypeInfo listElementTypeInfo = listTypeInfo.getListElementTypeInfo();
      Object[] values = new Object[list.size()];
      for (int i = 0; i < list.size(); i++) {
        values[i] = parseFieldValue(listElementTypeInfo, mapper.writeValueAsString(list.get(i)));
      }
      return Arrays.asList(values);
    } else if (typeInfo.getCategory().equals(Category.MAP)) {
      return mapper.readValue(stringValue, Map.class);
    } else if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
      return Byte.parseByte(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
      return Short.parseShort(stringValue);
    } else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
      return Date.valueOf(stringValue);
    } else {
      throw new UnsupportedOperationException("Unknown stringValue type: " + typeInfo);
    }
  }

  private CSVReader newReader(final Reader reader, char separator, char quote, char escape) {
    // CSVReader will throw an exception if any of separator, quote, or escape is the same, but
    // the CSV format specifies that the escape character and quote char are the same... very weird
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVReader(reader, separator, quote);
    } else {
      return new CSVReader(reader, separator, quote, escape);
    }
  }

  private CSVWriter newWriter(final Writer writer, char separator, char quote, char escape) {
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVWriter(writer, separator, quote, "");
    } else {
      return new CSVWriter(writer, separator, quote, escape, "");
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}