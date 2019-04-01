
public class IntegrationTests {

    @org.junit.ClassRule
    public static final PostgreSQLServer databaseServer = new PostgreSQLServer();
    
    @org.junit.Rule
    public final PostgreSQLServer.Wiper databaseWiper = new PostgreSQLServer.Wiper();

    
    @org.junit.Test
    public void aTest() {
      //...
    }
    
}