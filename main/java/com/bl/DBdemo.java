package com.bl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class DBdemo
{
	/**
	 * Main method for manipulation odf JDBC
	 * 
	 * @param args - Default Java param (Not used)
	 */
	public static void main(String[] args) {

		/**
		 * The connection URL for the mysql database is
		 * jdbc:mysql://localhost:3306/payroll_service where jdbc is the API, mysql is
		 * the database, localhost is the server name on which mysql is running, we may
		 * also use IP address, 3306 is the port number and payroll_service is the
		 * database name.
		 */
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";

		/**
		 * The default username for the mysql database is root.
		 */
		String userName = "root";

		/**
		 * It is the password given by the user at the time of installing the mysql
		 * database
		 */
		String password = "123456";
		Connection connection;

		/**
		 * try and catch block to handle the exceptions
		 */
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			System.out.println("Driver loaded");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Cannot find driver in classpath");
		}

		listDrivers();
		try {
			System.out.println("Connecting to database: " + jdbcURL);
			connection = DriverManager.getConnection(jdbcURL, userName, password);
			System.out.println("Connection successful: " + connection);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * created method listDrivers() to List the drivers being loaded
	 */
	private static void listDrivers() {

		/**
		 * Retrieving the list of all the Drivers
		 */
		Enumeration<Driver> driverList = DriverManager.getDrivers();

		/**
		 * Printing the list
		 */
		while (driverList.hasMoreElements()) {
			Driver driverClass = (Driver) driverList.nextElement();
			System.out.println("   " + driverClass.getClass().getName());
		}
	}
}