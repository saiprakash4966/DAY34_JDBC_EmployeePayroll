package com.bl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EmployeePayrollDBService
{
	public enum StatementType {
		PREPARED_STATEMENT, STATEMENT
	}

	private PreparedStatement employeePayrollDataStatement;
	private static EmployeePayrollDBService employeePayrollDBService;

	private EmployeePayrollDBService() {

	}

	/**
	 * For creating a singleton object
	 * 
	 * @return
	 */
	public static EmployeePayrollDBService getInstance() {
		if (employeePayrollDBService == null)
			employeePayrollDBService = new EmployeePayrollDBService();
		return employeePayrollDBService;
	}

	/**
	 * Read the employee payroll data from the database
	 * 
	 * @return
	 */
	public List<EmployeePayrollData> readData() {
		String sql = "SELECT * FROM employee_payroll;";
		return this.getEmployeePayrollDataUsingSQLQuery(sql);
	}

	/**
	 * To get the details of a particular employee from the DB using
	 * PreparedStatement Interface
	 */
	private void preparedStatementForEmployeeData() {
		try {
			Connection connection = this.getConnection();
			String sql = "Select * from employee_payroll WHERE name = ?";
			employeePayrollDataStatement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the list of EmployeePayrollData using the assigned name setString() is
	 * used to set the assigned name value in the sql query Return all the attribute
	 * values listed for a particular name
	 * 
	 * @param name
	 * @return
	 */
	public List<EmployeePayrollData> getEmployeePayrollData(String name) {
		List<EmployeePayrollData> employeePayrollList = null;
		if (this.employeePayrollDataStatement == null)
			this.preparedStatementForEmployeeData();
		try {
			employeePayrollDataStatement.setString(1, name);
			ResultSet resultSet = employeePayrollDataStatement.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	/**
	 * get the list of EmployeePayrollData Using the SQLQuery(
	 * 
	 * @param sql
	 * @return
	 */
	public List<EmployeePayrollData> getEmployeePayrollDataUsingSQLQuery(String sql) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<EmployeePayrollData>();
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	/**
	 * Assigning the value of the attributes in a list and return it
	 * 
	 * @param resultSet
	 * @return
	 */
	private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		try {
			while (resultSet.next()) {
				int id = resultSet.getInt("id");
				String name = resultSet.getString("name");
				double salary = resultSet.getDouble("salary");
				LocalDate startDate = resultSet.getDate("start").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(id, name, salary, startDate));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	/**
	 * Update the salary in the DB using Statement Interface
	 * 
	 * @param name
	 * @param salary
	 * @return
	 */
	public int updateEmployeeData(String name, double salary, StatementType type) {
		switch (type) {
		case STATEMENT:
			return this.updateDataUsingStatement(name, salary);
		case PREPARED_STATEMENT:
			return this.updateDataUsingPreparedStatement(name, salary);
		default:
			return 0;
		}
	}

	/**
	 * Update the salary in the DB using Statement Interface
	 * 
	 * @param name
	 * @param salary
	 * @return
	 */
	private int updateDataUsingStatement(String name, double salary) {
		String sql = String.format("UPDATE employee_payroll SET salary = %.2f where name = '%s';", salary, name);
		try (Connection connection = this.getConnection();) {
			Statement statement = connection.createStatement();
			return statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * Update the salary in the DB using Prepared Statement
	 * 
	 * @param name
	 * @param salary
	 * @return
	 */
	private int updateDataUsingPreparedStatement(String name, double salary) {
		String sql = "UPDATE employee_payroll SET salary = ? WHERE NAME = ?";
		try (Connection connection = this.getConnection();) {
			PreparedStatement preparedStatementUpdate = connection.prepareStatement(sql);
			preparedStatementUpdate.setDouble(1, salary);
			preparedStatementUpdate.setString(2, name);
			return preparedStatementUpdate.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * to retrieve all employees who have joined in a particular data range
	 * 
	 * @param date1
	 * @param date2
	 * @return employee list in given date range
	 */
	public List<EmployeePayrollData> getEmployeesInGivenDateRangeDB(String date1, String date2) {
		String sql = String.format("SELECT * FROM employee_payroll where start between '%s' AND '%s';", date1, date2);
		return this.getEmployeePayrollDataUsingSQLQuery(sql);
	}

	/**
	 * get the average salary group by the gender using hashmap that contains the
	 * key and value pair
	 * 
	 * @return
	 */
	public Map<String, Double> getAverageSalaryByGender() {
		String sql = "SELECT gender,AVG(salary) FROM employee_payroll GROUP BY gender;";
		Map<String, Double> genderToAvgSalaryMap = new HashMap<String, Double>();
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				String gender = resultSet.getString("gender");
				double salary = resultSet.getDouble("AVG(salary)");
				genderToAvgSalaryMap.put(gender, salary);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return genderToAvgSalaryMap;
	}

	/**
	 * Creating connection with the database
	 * 
	 * @return
	 * @throws SQLException
	 */
	private Connection getConnection() throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "123456";
		Connection connection;
		System.out.println("Connecting to database: " + jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("Connection successful: " + connection);
		return connection;
	}
}