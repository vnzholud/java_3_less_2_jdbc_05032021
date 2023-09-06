package com.gb;

import java.sql.*;

public class MainApp {
    private static Connection connection;
    private static Statement stmt;
    private static PreparedStatement psInsert;

    public static void main(String[] args) {

        try {
            connect();
            clearTable();
          prepareAllStatements();
//          exRollback();
//            exInsert();
//            exSelect();
            fillTable();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            disconnect();
        }
    }

    private static void exRollback() throws SQLException {
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob1', 95);");
        Savepoint sp1 = connection.setSavepoint();
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob2', 55);");
        connection.rollback(sp1);//отменили
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob3', 75);");
        connection.commit();
    }

    //откат при выполнении
    private static void batchFillTable() throws SQLException {
        long begin = System.currentTimeMillis();
        connection.setAutoCommit(false);// отключаем комит
        for (int i = 1; i < 10000; i++) {
            psInsert.setString(1, "Bob" + i);
            psInsert.setInt(2, i * 15 % 100);
            psInsert.addBatch(); //отправка одним пакетом для экономии трафика
        }
        psInsert.executeBatch();
        connection.commit();
        long end = System.currentTimeMillis();
        System.out.printf("Time: %d\n", end - begin);
    }

    private static void fillTable() throws SQLException {
        long begin = System.currentTimeMillis();
        connection.setAutoCommit(false);
        for (int i = 1; i < 10000; i++) {
            psInsert.setString(1, "Bob" + i);
            psInsert.setInt(2, i * 15 % 100);
            psInsert.executeUpdate();
        }

        connection.commit();
        long end = System.currentTimeMillis();
        System.out.printf("Time: %d\n", end - begin);
    }

    private static void prepareAllStatements() throws SQLException {
        psInsert = connection.prepareStatement("INSERT INTO students (name, score) VALUES ( ? , ? );");
    }

    //CRUD create read update delete
    private static void exSelect() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT name, score FROM students WHERE score < 50;");
        while (rs.next()) {
            System.out.println(rs.getString("name") + "  " + rs.getInt("score"));
        }
        rs.close();

    }

    private static void clearTable() throws SQLException {
        stmt.executeUpdate("DELETE FROM students;");
    }

    private static void exDelete() throws SQLException {
        stmt.executeUpdate("DELETE FROM students WHERE score = 100;");
    }

    private static void exUpdate() throws SQLException {
        stmt.executeUpdate("UPDATE students SET score = 100 WHERE score > 70;");
    }

    private static void exInsert() throws SQLException {
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob3', 75);");
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob4', 45),('Bob5', 35),('Bob6', 65);");
    }

    private static void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:main.db");
        stmt = connection.createStatement();
    }

    private static void disconnect() {
        try {
            stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
