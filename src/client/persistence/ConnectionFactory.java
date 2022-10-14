package client.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

	/**
	 * Name of the class that holds the jdbc driver implementation for the MySQL database
	 */
	private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	
	/**
	 * URI of the database to connect to
	 */
	private static final String DBURL = "jdbc:mysql://sepa.vaimee.it:3306/arces?useUnicode=true&serverTimezone=UTC";

	private static final String USERNAME = "admin";
	private static final String PASSWORD = "MySqlServer2020!";
	
	private static Connection conn = null;

	// --------------------------------------------

	static {
		try {
			Class.forName(DRIVER);
		} 
		catch (Exception e) {
			System.err.println("MySqlDAOFactory.class: failed to load MySQL JDBC driver\n"+e);
			e.printStackTrace();
		}
	}

	// --------------------------------------------
	
	public static Connection createConnection() {
		try {
			if (conn != null && !conn.isClosed()) return conn;
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		try {
			conn = DriverManager.getConnection (DBURL, USERNAME, PASSWORD);
			return conn;
		} 
		catch (Exception e) {
			System.err.println(ConnectionFactory.class.getName()+".createConnection(): failed creating connection\n"+e);
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	public static void closeConnection(Connection conn) {
		try {
			conn.close();
		}
		catch (Exception e) {
			System.err.println(ConnectionFactory.class.getName()+".closeConnection(): failed closing connection\n"+e);
			e.printStackTrace();
		}
	}

}
