package com.bl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

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
		employeePayrollService.updateEmployeeSalary("Mark Zuckerberg", 3000000.00);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark Zuckerberg");
		assertTrue(result);
		System.out.println(employeePayrollData);
	}	
}


