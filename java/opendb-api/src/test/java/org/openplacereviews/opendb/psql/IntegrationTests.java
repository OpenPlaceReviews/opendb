package org.openplacereviews.opendb.psql;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Ignore;


@Ignore
public class IntegrationTests {

    @org.junit.ClassRule
    public static final PostgreSQLServer databaseServer = new PostgreSQLServer();
    
//    @org.junit.Rule
//    public final PostgreSQLServer.Wiper databaseWiper = new PostgreSQLServer.Wiper();

    
    @org.junit.Test
    public void aTest() throws SQLException {
    	Connection c = databaseServer.getConnection();
    	c.createStatement().executeUpdate("CREATE TABLE A(a serial, b serial)");
    }
    
    @org.junit.Test
    public void bTest() throws SQLException {
    	Connection c = databaseServer.getConnection();
    	c.createStatement().executeUpdate("CREATE TABLE A(a serial, b serial)");
    }
    
}