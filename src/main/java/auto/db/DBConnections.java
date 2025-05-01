package auto.db;

import java.sql.*;

public class DBConnections {

    public ResultSet dbConn(String url, String username, String pswd, String query, String valueFiltering) throws SQLException {
        Connection conn = DriverManager.getConnection(url, username, pswd);
        PreparedStatement stm = conn.prepareStatement(query);
        stm.setString(1, valueFiltering);
        ResultSet rs = stm.executeQuery();
        return rs;
    }
    public ResultSet dbConn(String url, String username, String pswd, String query, int valueFiltering) throws SQLException {
        Connection conn = DriverManager.getConnection(url, username, pswd);
        PreparedStatement stm = conn.prepareStatement(query);
        stm.setInt(1, valueFiltering);
        ResultSet rs = stm.executeQuery();
        return rs;
    }

    public void closeConnection(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}