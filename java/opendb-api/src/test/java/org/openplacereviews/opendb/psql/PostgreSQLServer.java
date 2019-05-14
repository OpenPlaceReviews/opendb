package org.openplacereviews.opendb.psql;

import org.junit.runner.Description;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.sql.*;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.openplacereviews.opendb.VariableHelperTest.tables;

public class PostgreSQLServer extends org.junit.rules.ExternalResource {

	private static EmbeddedPostgres postgres;

	private static String JDBC_URL;
	private static final String JDBC_USERNAME = "my_test_username";
	private static final String JDBC_PASSWORD = "my_test_password";
	private static final String DB_NAME = "test_db";
	
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
	}

	private Collection<String> getAllDatabaseTables() throws SQLException {
		DatabaseMetaData md = getConnection().getMetaData();
		ResultSet rs = md.getTables(null, null, "%", new String[] { "TABLE" });
		while (rs.next()) {
			tables.add(rs.getString("TABLE_NAME"));
		}

		return tables;
	}

	@Override
	protected void before() throws Throwable {
		synchronized (PostgreSQLServer.class) {
			if (postgres == null) {
				postgres = new EmbeddedPostgres();
				postgres.start("127.0.0.1", 25512, DB_NAME, JDBC_USERNAME, JDBC_PASSWORD);
				JDBC_URL = postgres.getConnectionUrl().orElseThrow(
						() -> new IllegalStateException("Failed to get PostgreSQL Connection URL"));
				// Register hook to shutdown the
				// PostgreSQL Embedded server at JVM shutdown.
				Runtime.getRuntime().addShutdownHook(
						new Thread(() -> Optional.ofNullable(postgres).ifPresent(EmbeddedPostgres::stop)));
			}
		}
		System.setProperty("spring.datasource.url", JDBC_URL);
		System.setProperty("spring.datasource.username", JDBC_USERNAME);
		System.setProperty("spring.datasource.password", JDBC_PASSWORD);
	}
	
	@Override
	protected void after() {
		super.after();
		postgres.stop();
	}

	public static class Wiper implements org.junit.rules.TestRule {

		@Override
		public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base, Description description) {
			return new org.junit.runners.model.Statement() {
				@Override
				public void evaluate() throws Throwable {
					try {
						before();
						base.evaluate();
					} finally {
						after();
					}
				}
			};
		}

		private void before() {
			// Discussed later in the "Test" section
		}

		private void after() {
			// Discussed later in the "Test" section
		}

	}

	public void wipeDatabase() throws Exception {
		synchronized (PostgreSQLServer.class) {
			try (final Connection connection = getConnection();
				 final java.sql.Statement databaseTruncationStatement = connection.createStatement()) {
				databaseTruncationStatement.execute("BEGIN TRANSACTION");
				databaseTruncationStatement.execute(String.format("TRUNCATE %s RESTART IDENTITY CASCADE",
						String.join(",", getAllDatabaseTables())));
				databaseTruncationStatement.execute("COMMIT TRANSACTION"); // Reset constraints
			}
		}
	}
	
	public void deleteAllTables() throws SQLException {
		synchronized (PostgreSQLServer.class) {
			try (final Connection connection = getConnection()) {
				final java.sql.Statement databaseTruncationStatement = connection.createStatement();
				// Disable all constraints
				databaseTruncationStatement.execute("SET session_replication_role = replica"); 
				databaseTruncationStatement.execute("BEGIN TRANSACTION");
				final Set<String> temporaryTablesStatements = new TreeSet<>();
				int index = 0;
				final Collection<String> allDatabaseTables = getAllDatabaseTables();
				for (final String table : allDatabaseTables) {
					// Much faster to delete all tables in a single statement
					temporaryTablesStatements.add(String.format("table_%s AS (DELETE FROM %s)", index++, table));
				}
				databaseTruncationStatement.execute(String.format("WITH %S SELECT 1",
						String.join(",", temporaryTablesStatements)));
				databaseTruncationStatement.execute("COMMIT TRANSACTION");
				databaseTruncationStatement.execute("SET session_replication_role = DEFAULT"); // Reset constraints
			}
		}
	}
}
