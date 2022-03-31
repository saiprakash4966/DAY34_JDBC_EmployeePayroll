package com.bl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.awt.Window.Type;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import com.bl.EmployeePayrollDBService.StatementType;
import com.bl.EmployeePayrollService.IOService;

public class EmployeePayrollTest 
{

	@Test
	/**
	 * created test method to match the entries
	 */
	public void given3Employees_WhenWrittenToFile_ShouldMatchEmployeeEntries() {
		EmployeePayrollData[] arrayOfEmp = { new EmployeePayrollData(1, "Jeff Bezos", 100000.0),
				new EmployeePayrollData(2, "Bill Gates", 200000.0),
				new EmployeePayrollData(3, "Mark Zuckerberg", 300000.0) };
		EmployeePayrollService employeePayrollService;
		employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		employeePayrollService.writeEmployeeData(IOService.FILE_IO);
		long entries = employeePayrollService.countEntries(IOService.FILE_IO);
		employeePayrollService.printData(IOService.FILE_IO);
		List<EmployeePayrollData> employeeList = employeePayrollService.readData(IOService.FILE_IO);
		System.out.println(employeeList);
		assertEquals(3, entries);
	}

	@Test
	/**
	 * To check the count in database is matching in java program or not
	 */
	public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO);
		assertEquals(3, employeePayrollData.size());
	}

	@Test
	/**
	 * To check whether the salary is updated in the database and is synced with the
	 * DB
	 * 
	 * @throws EmployeePayrollException
	 */
	public void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDatabase() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO);
		employeePayrollService.updateEmployeeSalary("Mark Zuckerberg", 3000000.00, StatementType.STATEMENT);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark Zuckerberg");
		assertTrue(result);
		System.out.println(employeePayrollData);
	}

	@Test
	/**
	 * To test whether the salary is updated in the database and is synced with the
	 * DB using JDBC PreparedStatement
	 * 
	 * @throws EmployeePayrollException
	 */
	public void givenNewSalaryForEmployee_WhenUpdatedUsingPreparedStatement_ShouldSyncWithDatabase()
			throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO);
		employeePayrollService.updateEmployeeSalary("Mark Zuckerberg", 3000000.00, StatementType.PREPARED_STATEMENT);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark Zuckerberg");
		assertTrue(result);
		System.out.println(employeePayrollData);
	}

	@Test
	/**
	 * To test whether the count of the retrieved data who have joined in a
	 * particular data range matches with the expected value
	 * 
	 * @throws EmployeePayrollException
	 */
	public void givenDateRangeForEmployee_WhenRetrievedUsingStatement_ShouldReturnProperData()
			throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO);
		List<EmployeePayrollData> employeeDataInGivenDateRange = employeePayrollService
				.getEmployeesInDateRange("2018-01-01", "2019-11-15");
		assertEquals(2, employeeDataInGivenDateRange.size());
		System.out.println(employeePayrollData);
	}

}