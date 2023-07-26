package dev.omedia;

import dev.omedia.annos.Column;
import dev.omedia.annos.Id;
import dev.omedia.annos.Table;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class SQLCommands {

    public static boolean insert(Object o) {
        Class<?> clazz = o.getClass();
        List<String> values = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        fillLists(clazz, o, values, colNames);

        String sqlValues = String.join(",", values);
        String sqlColNames = String.join(",", colNames);

        String query = insertQuery(clazz, sqlColNames, sqlValues);
        return prepareMy(query);
    }

    private static String insertQuery(Class<?> clazz, String sqlColNames, String sqlValues) {
        return "INSERT INTO " + clazz.getAnnotation(Table.class).scheme() + "."
                + clazz.getAnnotation(Table.class).name() +
                " (" + sqlColNames + ")" + " VALUES(" + sqlValues + ");";
    }


    private static boolean prepareMy(String query) {
        try (Connection con = JDBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = con.prepareStatement(query);
            return preparedStatement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void fillLists(Class<?> clazz, Object o, List<String> values, List<String> colNames) {
        List<Field> colFields = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Column.class))
                .collect(Collectors.toList());

        colFields.stream().forEach(field -> {
            try {
                fieldValue(field, o, values, colNames);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void fieldValue(Field field, Object o, List<String> values, List<String> colNames)
            throws IllegalAccessException {
        field.setAccessible(true);
        if (o instanceof Number) {
            values.add("" + field.get(o));
        } else {
            values.add("'" + field.get(o) + "'");
        }
        colNames.add(field.getAnnotation(Column.class).name());
    }

    private static String updateQuery(Class<?> clazz, String colStatement, String idStatement) {
        return "UPDATE " + clazz.getAnnotation(Table.class).scheme() + "."
                + clazz.getAnnotation(Table.class).name()
                + " SET " + colStatement + " WHERE " + idStatement + ";";
    }

    public static boolean update(Object o) {
        Class<?> clazz = o.getClass();

        List<String> values = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        fillLists(clazz, o, values, colNames);

        List<String> idNames = new ArrayList<>();
        List<String> idValues = new ArrayList<>();
        fillIdLists(clazz, o, idNames, idValues);

        List<String> colVal = fillVal(colNames, values);
        List<String> idVal = fillVal(idNames, idValues);

        String colStatement = String.join(",", colVal);
        String idStatement = String.join(",", idVal);

        String query = updateQuery(clazz, colStatement, idStatement);
        return prepareMy(query);
    }

    private static List<String> fillVal(List<String> names, List<String> values) {
        AtomicInteger i = new AtomicInteger();
        return values.stream()
                .map(value -> names.get(i.getAndIncrement()) + "=" + value)
                .collect(Collectors.toList());
    }

    private static void fillIdLists(Class<?> clazz, Object o, List<String> idNames, List<String> idValues) {
        Arrays.stream(clazz.getDeclaredFields()).filter(field -> field.isAnnotationPresent(Id.class))
                .forEach(field -> {
                    try {
                        fieldIdValue(field, o, idValues, idNames);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void fieldIdValue(Field field, Object o, List<String> idValues, List<String> idNames)
            throws IllegalAccessException {
        field.setAccessible(true);
        if (o instanceof Number) {
            idValues.add("" + field.get(o));
        } else {
            idValues.add("'" + field.get(o) + "'");
        }
        idNames.add(field.getAnnotation(Id.class).name());
    }

    public static boolean delete(Object o) {
        Class<?> clazz = o.getClass();

        List<String> values = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        fillLists(clazz, o, values, colNames);

        List<String> idNames = new ArrayList<>();
        List<String> idValues = new ArrayList<>();
        fillIdLists(clazz, o, idNames, idValues);

        List<String> colVal = fillVal(colNames, values);
        List<String> idVal = fillVal(idNames, idValues);
        colVal.addAll(idVal);

        String colStatement = String.join(" AND ", colVal);

        String query = deleteQuery(clazz, colStatement);
        return prepareMy(query);
    }

    private static String deleteQuery(Class<?> clazz, String colStatement) {
        return "DELETE FROM " + clazz.getAnnotation(Table.class).scheme()
                + "." + clazz.getAnnotation(Table.class).name()
                + " WHERE " + colStatement + ";";
    }

    public static <E> boolean delete(Class<E> o, Object id) {
        String idName = getIdName(o);

        String query = deleteQuery(o, idName + "=" + id);
        return prepareMy(query);
    }


    public static <E> List<E> select(Class<E> o) {
        String scheme = o.getAnnotation(Table.class).scheme();
        String name = o.getAnnotation(Table.class).name();

        List<String> colNames = fillCols(o);

        Constructor<?> cons = findCons(o, colNames.size());

        String query = "SELECT * FROM " + scheme + "." + name;
        List<E> result = new ArrayList<>();
        return connectToBase(query, colNames, result, cons);
    }

    private static <E> List<E> connectToBase(String query, List<String> colNames, List<E> result, Constructor<?> cons) {
        try (Connection con = JDBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = con.prepareStatement(query);
            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                Object[] objs = new Object[colNames.size()];
                for (int i = 0; i < colNames.size(); i++) {
                    objs[i] = rs.getObject(colNames.get(i));
                }
                result.add((E) cons.newInstance(objs));

            }
            return result;
        } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    private static <E> Constructor<?> findCons(Class<E> o, int size) {
        return Arrays.stream(o.getConstructors())
                .filter(constructor -> constructor.getParameterCount() == size)
                .findFirst()
                .orElseThrow();
    }

    private static <E> List<String> fillCols(Class<E> o) {
        return Arrays.stream(o.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Column.class) || field.isAnnotationPresent(Id.class))
                .map(field -> {
                    if (field.isAnnotationPresent(Column.class)) {
                        return field.getAnnotation(Column.class).name();
                    } else {
                        return field.getAnnotation(Id.class).name();
                    }
                }).collect(Collectors.toList());
    }

    public static <E> E selectbyId(Class<E> o, Object id) {
        String scheme = o.getAnnotation(Table.class).scheme();
        String name = o.getAnnotation(Table.class).name();
        String idName = getIdName(o);
        List<String> colNames = fillCols(o);

        Constructor<?> cons = findCons(o, colNames.size());

        String query = "SELECT * FROM " + scheme + "." + name + " WHERE " + idName + " = " + id;
        return connectToData(query, colNames, cons);
    }

    private static <E> String getIdName(Class<E> o) {
        return Arrays.stream(o.getDeclaredFields()).filter(field -> field.isAnnotationPresent(Id.class))
                .findFirst()
                .orElseThrow()
                .getAnnotation(Id.class).name();
    }

    private static <E> E connectToData(String query, List<String> colNames, Constructor<?> cons) {
        E result = null;
        try (Connection con = JDBConnection.INSTANCE.getConnection()) {
            PreparedStatement preparedStatement = con.prepareStatement(query);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                Object[] obj = new Object[colNames.size()];
                for (int i = 0; i < colNames.size(); i++) {
                    obj[i] = rs.getObject(colNames.get(i));
                }
                result = ((E) cons.newInstance(obj));
                break;
            }
            return result;
        } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
