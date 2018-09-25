package com.java.infanticida;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author wislanildo
 * <p>
 * Classe gera o SQL de deleção necessário para remover tuplas que estejam sendo referenciadas por outras tabelas,
 * gerando SQL de deleção também para todas as tabelas relacionadas.
 * <p>
 * TODO: Não funciona se o id da tabela possuir chave composta.
 */
public class Main {

    private final static String USER = "usuario";
    private final static String PASSWD = "senha";
    private final static String DB_URL = "jdbc:oracle:thin:@172.0.0.1:1521:SeuDB";
    private static Connection connection;

    public static void main(String[] args) throws Exception {

        List<String> deletes = queryOfParentsToDelete("EVENTO", "ID = 123" );

        for (String delete : deletes) {
            System.out.println(delete);
        }

    }

    private static List<String> queryOfParentsToDelete(String tableName, String whereClause) throws Exception {

        List<String> deletes = new ArrayList<String>();

        ResultSet result = null;

        try {

            String pkColumName = getPKColumName(tableName);

            result = getConnection().prepareStatement("SELECT "+ pkColumName + " FROM " + tableName + " WHERE " + whereClause).executeQuery();

            List<Integer> idsParent = new ArrayList<Integer>();

            while (result.next()) {
                idsParent.add(result.getInt(pkColumName));
            }
            finalizaResultSet(result);

            for (Integer id : idsParent) {
                delete(deletes, pkColumName, id, tableName);
                deletes.add("----------------------------------------------");
            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }


        return deletes;

    }

    private static void delete(List<String> deletes, String pkColumName, Integer idParent, String tableParent) throws Exception {

        gerarSQLUpdateParaNullColunasReferenciamOutrasTablelas(deletes, pkColumName, tableParent, idParent);

        deletarDependentes(deletes, idParent, tableParent, tableParent);
        deletes.add("DELETE FROM " + tableParent + " WHERE " + pkColumName + " = " + idParent + ";");
    }

    private static List<String> deletarDependentes(List<String> deletes, Integer idParent, String tableParent, String patriarchTableName) throws Exception {

        Map<String, String> childInformation = getChildInformation(tableParent);

        List<String> deletesPostPatriarch = new ArrayList<String>();

        for (Map.Entry<String, String> entry : childInformation.entrySet()) {

            String constraintName = entry.getKey();
            String childTableName = entry.getValue();

            String childColum = getFKColum(constraintName, childTableName);
            Integer childId = getChildId(childTableName, childColum, idParent);

            if (childId != null && !childTableName.equals(patriarchTableName)) {

                deletarDependentes(deletes, childId, childTableName, patriarchTableName);

                deletes.add("DELETE FROM " + childTableName + " WHERE "+ childColum.toUpperCase() +"=" + idParent + ";");

            }

        }

        return deletesPostPatriarch;

    }
    private static void gerarSQLUpdateParaNullColunasReferenciamOutrasTablelas(List<String> deletes, String pkColumName, String parentTableName, Integer parentId) throws Exception {

        ResultSet result = null;
        Map<String, String> map = new HashMap<String, String>();

        try {

            String sql = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + parentTableName + "' AND NULLABLE = 'Y'";

            result = getConnection().prepareStatement(sql).executeQuery();

            List<String> colunas = new ArrayList<String>();

            while (result.next()) {

                colunas.add(result.getString("COLUMN_NAME"));
            }

            if(!colunas.isEmpty()){

                StringBuilder sqlUpdate = new StringBuilder("UPDATE " + parentTableName.toUpperCase() + " SET ");

                int count = 0;

                for(String colum : colunas){
                    count ++;

                    sqlUpdate.append( colum + "=''");

                    if (count < colunas.size()){
                        sqlUpdate.append(", ");
                    }
                }

                sqlUpdate.append(" WHERE "+ pkColumName +" = " + parentId + ";");

                deletes.add(sqlUpdate.toString());
            }


        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            finalizaResultSet(result);
        }

    }


    private static Map<String, String> getChildInformation(String tableName) throws Exception {

        ResultSet result = null;
        Map<String, String> map = new HashMap<String, String>();

        try {

            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(" SELECT AC.CONSTRAINT_NAME, AC.TABLE_NAME ");
            strBuilder.append(" FROM ALL_CONSTRAINTS AC ");
            strBuilder.append(" WHERE AC.R_CONSTRAINT_NAME IN (SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE TABLE_NAME='" + tableName + "') ");

            result = getConnection().prepareStatement(strBuilder.toString()).executeQuery();

            while (result.next()) {
                map.put(result.getString("CONSTRAINT_NAME"), result.getString("TABLE_NAME"));
            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            finalizaResultSet(result);
        }

        return map;

    }

    private static String getPKColumName(String tableName) throws Exception {

        ResultSet result = null;

        try {

            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(" SELECT cols.column_name ");
            strBuilder.append(" FROM all_constraints cons, all_cons_columns cols ");
            strBuilder.append(" WHERE cols.table_name = '"+ tableName +"' ");
            strBuilder.append(" AND cons.constraint_type = 'P' ");
            strBuilder.append(" AND cons.constraint_name = cols.constraint_name ");
            strBuilder.append(" AND cons.owner = cols.owner ");

            result = getConnection().prepareStatement(strBuilder.toString()).executeQuery();

            if (result.next()) {
                return result.getString("COLUMN_NAME");
            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            finalizaResultSet(result);
        }

        return null;

    }

    private static String getFKColum(String fkName, String fkTableName) throws Exception {

        ResultSet result = null;

        try {

            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append(" SELECT ACC.COLUMN_NAME ");
            strBuilder.append(" FROM ALL_CONS_COLUMNS ACC ");
            strBuilder.append(" INNER JOIN ALL_CONSTRAINTS AC ON ( ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME ) ");
            strBuilder.append(" WHERE AC.CONSTRAINT_NAME = '" + fkName + "' ");
            strBuilder.append(" AND AC.TABLE_NAME = '" + fkTableName + "' ");

            result = getConnection().prepareStatement(strBuilder.toString()).executeQuery();

            if (result.next()) {
                return result.getString("COLUMN_NAME");
            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            finalizaResultSet(result);
        }

        return null;

    }

    public static Integer getChildId(String childTable, String childColum, Integer parentId) throws Exception {
        ResultSet result = null;

        try {

            String pkColumName = getPKColumName(childTable);

            String query = "SELECT "+ pkColumName +" FROM " + childTable + " WHERE " + childColum + " = " + parentId;

            result = getConnection().prepareStatement(query).executeQuery();

            if (result.next()) {
                return result.getInt(pkColumName);

            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
            finalizaResultSet(result);
        }

        return null;
    }

    public static Connection getConnection() throws Exception {

        if (connection == null) {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                connection = DriverManager.getConnection(DB_URL, USER, PASSWD);
            } catch (Exception e) {
                throw new Exception(e.getMessage());
            }
        }

        return connection;

    }

    private static void finalizaResultSet(ResultSet result) throws Exception {
        try {
            if (result != null) {
                result.close();
                result.getStatement().close();
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

}
