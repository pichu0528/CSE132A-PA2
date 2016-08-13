/* Author: Pin Chu
 * PID   : A98041513
 * Due   : 29 Feburary 2016 11:59pm
 */

/**
 * This Java program exemplifies the basic usage of JDBC.
 * Requirements:
 * (1) JDK 1.6+.
 * (2) SQLite3.
 * (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlitejdbc/downloads/sqlite-jdbc-3.8.7.jar).
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PA2 {
    public static void main(String[] args) {
        Connection conn = null; // Database connection.

        try {
            // Load the JDBC class.
            Class.forName("org.sqlite.JDBC");
            
        // Get the connection to the database.
            // - "jdbc" : JDBC connection name prefix.
            // - "sqlite" : The concrete database implementation
            // (e.g., sqlserver, postgresql).
            // - "pa2.db" : The name of the database. In this project,
            // we use a local database named "pa2.db". This can also
            // be a remote database name.
            conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");
            System.out.println("Opened database successfully.");
            
            // Use case #1: Create and populate a table.
            // Get a Statement object.
            Statement stmt = conn.createStatement();

        // Drop all the table that might have been existed before 
        stmt.executeUpdate("DROP TABLE IF EXISTS Current;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Same_Flights;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Previous;"); 
            stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");

            // Create necessary tables for computing purposes
            // Current        : temporary input graph
            // Same_Flights: new tuples added to T of previous iteration
            // Previous    : transitive closure
            stmt.executeUpdate(
                "CREATE TABLE Current(Airline char(32), Origin char(32), Destination char(32), Stops int);");
            stmt.executeUpdate(
                "CREATE TABLE Same_Flights(Airline char(32), Origin char(32), Destination char(32));");
            stmt.executeUpdate(
                "CREATE TABLE Previous(Airline char(32), Origin char(32), Destination char(32), Stops int);");
            
            // Insert tables with appropriate information
            stmt.executeUpdate(
                "INSERT INTO Current(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");
            stmt.executeUpdate(
                "INSERT INTO Same_Flights(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");

        /*********** Given code from the handout ***********/
            // Use case #2: Query the Student table with Statement.
            // Returned query results are stored in a ResultSet
            // object.
            // ResultSet rset = stmt.executeQuery("SELECT * from Student;");
            
            // Print the FirstName and LastName columns.
            // System.out.println ("\nStatement result:");
           
            // This shows how to traverse the ResultSet object.
            /* while (rset.next()) {
 
                 // Get the attribute value.
                 System.out.print(rset.getString("FirstName"));
                 System.out.print("---");
                 System.out.println(rset.getString("LastName"));
             }
 
             // Use case #3: Query the Student table with
             // PreparedStatement (having wildcards).
             PreparedStatement pstmt = conn.prepareStatement(
                 "SELECT * FROM Student WHERE FirstName = ?;");

             // Assign actual value to the wildcard.
             pstmt.setString (1, "F1");
             rset = pstmt.executeQuery ();
             System.out.println ("\nPrepared statement result:");
             while (rset.next()) {
                 System.out.print(rset.getString("FirstName"));
                 System.out.print("---");
                 System.out.println(rset.getString("LastName"));
             }
            */
        /***************************************************/
            
        // Note: executeUpdate returns an integer representing the num of rows 
            //       affected by the SQL statement.
        // ResultSet object that I will be using to track the number of flights
            ResultSet rset = stmt.executeQuery("SELECT COUNT(*) AS Size FROM Same_Flights;");
        // Variables that I will use to derive the minimum stops for Connected table
            int num_stops   = 0;
            int num_flights = rset.getInt("Size");
        
            /*
              <pseudo-code>
              T     := G // BASE CASE
              Delta := G
              while delta != $ do // loop until no new tuples are generated
              {
                Told = T
                T := (SELECT * FROM T)
                UNION
                (SELECT x.A, y.B FROM G x, Delta y
                 WHERE x.B = y.A)
                Delta := T - Told
              }
              Output T
             */
            while(num_flights > 0){
                if(num_stops > 0){
                    stmt.executeUpdate("DELETE FROM Previous;");
                    stmt.executeUpdate("INSERT INTO Previous SELECT * FROM Current;");
                    stmt.executeUpdate("INSERT INTO Current SELECT s.Airline, s.Origin, f.Destination,"+num_stops+
                        " FROM Same_Flights s, Flight f" +
                        " WHERE s.Destination = f.Origin AND" +
                        " s.Airline = f.Airline AND" +
                        " s.Origin <> f.Destination;");
                    stmt.executeUpdate("DELETE FROM Same_Flights;");
                    stmt.executeUpdate(
                        "INSERT INTO Same_Flights SELECT Airline, Origin, Destination" + 
                        " FROM Current" + 
                        " EXCEPT SELECT Airline, Origin, Destination" + 
                        " FROM Previous;");

                }
                else{
                    stmt.executeUpdate("INSERT INTO Current SELECT *," + num_stops +" FROM Flight;");
                    stmt.executeUpdate("DELETE FROM Current WHERE Current.Stops IS NULL;");
                }

                rset = stmt.executeQuery("SELECT COUNT(*) AS Size FROM Same_Flights;");
                num_flights = rset.getInt("Size");
                num_stops++;
            }

            // Create the output table Connected
            stmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops int);");
            stmt.executeUpdate("INSERT INTO Connected" + 
                " SELECT Airline, Origin, Destination, MIN(Stops)" + 
                " FROM Current" +
                " GROUP BY Airline, Origin, Destination" + 
                " ORDER BY Airline;");

            // Drop all the temporary tables
            stmt.executeUpdate("DROP TABLE IF EXISTS Same_Flights;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Previous;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Current;");

            // Close the ResultSet and Statement objects.
            rset.close();
            stmt.close();
        } catch (Exception e) {
            throw new RuntimeException("There was a runtime problem!", e);
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(
                    "Cannot close the connection!", e);
            }
        }
 }
}

