package com.techwalk.cassandra.Techwalk_Cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraTest {

	public static void main(String[] args) {
		final CassandraConnection client = new CassandraConnection();
		final String ipAddress = args.length > 0 ? args[0] : "localhost";
		final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
		System.out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
		client.connect(ipAddress, port);
		// get session to perform curd operation.
		Session session = client.getSession();

		// we have one employee table in our database.

		final String insertEmp = "INSERT INTO TechwalkDB.TechwalkUser (userid , firstname , lastname , salary ) VALUES ( 5,'Harshverdhan','Chauhan',6000)";
		session.execute(insertEmp);

		// Use select to get the user we just entered
		ResultSet results = session
				.execute("SELECT * FROM TechwalkDB.TechwalkUser WHERE lastname='Chauhan' ALLOW FILTERING");
		for (Row row : results) {
			System.out.println(row.getString("firstname") + " -> " + row.getDouble("salary"));
		}

		// Update the same user with a new salary
		session.execute("update TechwalkDB.TechwalkUser set salary = 8000 where userid = 5");

		// Select and show the change
		results = session.execute("select * from TechwalkDB.TechwalkUser where lastname='Chauhan' ALLOW FILTERING");
		for (Row row : results) {
			System.out.println(row.getString("firstname") + " -> " + row.getDouble("salary"));
		}

		// Delete the user from the users table
		session.execute("DELETE FROM TechwalkDB.TechwalkUser where userid = 5");

		client.close();
	}
}
