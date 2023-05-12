import java.sql.*;
import java.io.*;
import java.time.Period;

public class Transaction {

    Connection conn = null;
    Statement stmt = null;
    static BufferedReader in = null;

    static final String URL = "jdbc:mysql://localhost/";
    static final String BD = "sys";		// especificar: el nombre de la DB,
    static final String USER = "root";		// el nombre de usuario
    static final String PASSWD = "5454";// el password del usuario

    public Transaction() throws SQLException, Exception {

        // this will load the MySQL driver, each DB has its own driver
        Class.forName( "com.mysql.cj.jdbc.Driver" );
        System.out.print( "Connecting to the database... " );

        // setup the connection with the DB
        conn = DriverManager.getConnection( URL+BD, USER, PASSWD );
        System.out.println( "connected\n\n" );

        conn.setAutoCommit( false );         // inicio de la 1a transacción
        stmt = conn.createStatement();
        in = new BufferedReader( new InputStreamReader(System.in) );

    }

    private void dumpResultSet( ResultSet rset ) throws SQLException {

        ResultSetMetaData rsetmd = rset.getMetaData();
        int i = rsetmd.getColumnCount();

        while( rset.next() ) {

            for( int j = 1; j <= i; j++ ) {
                System.out.print( rset.getString(j) + "\t" );
            }
            System.out.println();
        }
    }

    private void query( String statement ) throws SQLException {

        ResultSet rset = stmt.executeQuery( statement );
        System.out.println( "Results:" );
        dumpResultSet( rset );

        System.out.println();
        rset.close();
    }

    private void close() throws SQLException {

        stmt.close();
        conn.close();
    }

    private boolean menuPeriod() throws Exception {

        String statement;

        System.out.println( "\nNivel de aislamiento = " + conn.getTransactionIsolation() + "\n" );
        System.out.println( "(1) Lista completa\n" );
        System.out.println( "(2) Lista restringida\n" );
        System.out.println( "(3) Insertar un periodo\n" );
        System.out.println( "(4) Borrar un periodo\n" );
        System.out.println( "(5) Ir a todas las tablas\n" );
        System.out.println( "(6) Salir\n\n" );
        System.out.print( "Option: " );

        switch( Integer.parseInt( "0" + in.readLine() ) ) {

            case 1:	query( "select * from PERIOD" );
                break;

            case 2:	System.out.println( "\nPredicado?" );
                query( "select * from PERIOD where " + in.readLine() );
                break;

            case 3:	statement = "insert into PERIOD values ( ";

                System.out.println( "\nTitle?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Startdate?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Enddate?" );
                statement += "'" + in.readLine() + "')";

                System.out.println(statement);

                stmt.executeUpdate( statement );

                conn.commit();

                break;

            case 4:	System.out.println( "\nPeriod?" );
                stmt.executeUpdate( "delete from PERIOD where TITLE = '" + in.readLine() + "'" );
                break;

            case 5:
                whichTable();
                return false;


            case 6:
                return false;
        }

        return true;
    }//end menuPeriod

    private boolean menuRoom() throws Exception {

        String statement;

        System.out.println( "\nNivel de aislamiento = " + conn.getTransactionIsolation() + "\n" );
        System.out.println( "(1) Lista completa\n" );
        System.out.println( "(2) Lista restringida\n" );
        System.out.println( "(3) Insertar un Room\n" );
        System.out.println( "(4) Borrar un Room\n" );
        System.out.println( "(5) Ir a todas las tablas\n" );
        System.out.println( "(6) Salir\n\n" );
        System.out.print( "Option: " );

        switch( Integer.parseInt( "0" + in.readLine() ) ) {

            case 1:	query( "select * from ROOM" );
                break;

            case 2:	System.out.println( "\nPredicado?" );
                query( "select * from ROOM where " + in.readLine() );
                break;

            case 3:	statement = "insert into ROOM values ( ";


                System.out.println( "\nRoomID?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Capacity?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Room Type?" );
                statement += "'" + in.readLine() + "')";

                stmt.executeUpdate( statement );
                conn.commit();
                break;

            case 4:	System.out.println( "\nRoom?" );
                stmt.executeUpdate( "delete from ROOM where ROOMID = '" + in.readLine() + "'" );
                conn.commit();
                break;

            case 5:
                whichTable();
                return false;


            case 6:
                return false;
        }

        return true;
    }//end menuRoom

    private boolean menuReservation() throws Exception {

        String statement;
        String date;
        String roomID;


        System.out.println( "\nNivel de aislamiento = " + conn.getTransactionIsolation() + "\n" );
        System.out.println( "(1) Lista completa\n" );
        System.out.println( "(2) Lista restringida\n" );
        System.out.println( "(3) Insertar un Reservation\n" );
        System.out.println( "(4) Cancel a reservation in a specific Date and Time\n" );
        System.out.println( "(5) Cancel a reservation between dates\n" );
        System.out.println( "(6) Rooms available in a specified weekday\n" );
        System.out.println( "(7) Available rooms Capacity\n" );
        System.out.println( "(8) Reserve a room for an entire period\n" );
        System.out.println( "(9) Ir a todas las tablas\n" );
        System.out.println( "(10) Salir\n\n" );
        System.out.print( "Option: " );

        switch( Integer.parseInt( "0" + in.readLine() ) ) {

            case 1:	query( "select * from RESERVATION" );
                break;

            case 2:	System.out.println( "\nPredicado?" );
                query( "select * from RESERVATION where " + in.readLine() );
                break;

            case 3:	statement = "insert into RESERVATION values ( ";


                System.out.println( "\nDate? Format: YYYY-MM-DD HH:MM:SS" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Name?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Period?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Weekday?" );
                statement += "'" + in.readLine() + "',";

                statement += "'" + 60 + "',";

                System.out.println( "Room ID?" );
                statement += "'" + in.readLine() + "')";


                try
                {
                    stmt.executeUpdate( statement );
                } catch (SQLException ex)
                {
                    System.out.println(ex.getMessage());
                }
                conn.commit();
                break;

            case 4:	System.out.println( "\nReservation date and time? Format: YYYY-MM-DD HH:MM:SS" );

                date = in.readLine();

                System.out.println( "\nRoom ID?" );

                roomID = in.readLine();

                stmt.executeUpdate( "delete from RESERVATION where RTIME = '" + date + "' and ROOMID = '" + roomID + "'");

                conn.commit();
                break;

            case 5:	System.out.println( "\nDate?");

                date = in.readLine();

                System.out.println( "\nRoom ID?" );

                roomID = in.readLine();

                System.out.println( "After? or before a date. ('A' for After, 'B' for before )");
                if(in.readLine() == "A")
                {

                    stmt.executeUpdate( "delete from RESERVATION where RTIME > '" + date + "' and RTIME != '0000-01-01 00:00:01' and ROOMID = '" + roomID + "' ");

                }//end if
                else
                {

                    stmt.executeUpdate( "delete from RESERVATION where RTIME < '" + date + "' and RTIME != '0000-01-01 00:00:01' and ROOMID = '" + roomID + "' ");

                }//end else

                conn.commit();

                break;

            case 6:	System.out.println( "\nWeekday?" );
                query( "select R.ROOMID from ROOM as R where R.ROOMID not in (select ROOMID from SCHEDULE as S where S.WEEKDAY = '" + in.readLine() + "')" );
                break;

            case 7:	System.out.println( "\nWeekday?" );
                query( "select R.ROOMID, CAPACITY from ROOM as R where R.ROOMID not in (select ROOMID from SCHEDULE as S where S.WEEKDAY = '" + in.readLine() + "')" );
                break;

            case 8:

                statement = "insert into RESERVATION values ( ";

                System.out.println( "\nTime? Format: HH:MI:SS" );
                statement += " '0000-01-01" + in.readLine() + "',";

                System.out.println( "Name?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Period?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Weekday?" );
                statement += "'" + in.readLine() + "',";

                statement += "'" + 60 + "',";

                System.out.println( "Room?" );
                statement += "'" + in.readLine() + "')";

                System.out.println(statement);

                stmt.executeUpdate( statement );

                conn.commit();

                break;

            case 9:
                whichTable();
                return false;


            case 10:
                return false;
        }

        return true;
    }//end menuReservation

    private boolean menuCourse() throws Exception {

        String statement;
        String courseKey;
        int section;


        System.out.println( "\nNivel de aislamiento = " + conn.getTransactionIsolation() + "\n" );
        System.out.println( "(1) Lista completa\n" );
        System.out.println( "(2) Lista restringida\n" );
        System.out.println( "(3) Insertar un Course\n" );
        System.out.println( "(4) Borrar un Course\n" );
        System.out.println( "(5) Ir a todas las tablas\n" );
        System.out.println( "(6) Salir\n\n" );
        System.out.print( "Option: " );

        switch( Integer.parseInt( "0" + in.readLine() ) ) {

            case 1:	query( "select * from COURSE" );
                break;

            case 2:	System.out.println( "\nPredicado?" );
                query( "select * from COURSE where " + in.readLine() );
                break;

            case 3:	statement = "insert into COURSE values ( ";

                System.out.println( "\nCourse key?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Section?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Course Title?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Professor?" );
                statement += "'" + in.readLine() + "')";

                stmt.executeUpdate( statement );
                conn.commit();
                break;

            case 4:	System.out.println( "\nCourse Key?" );
                courseKey = in.readLine();

                System.out.println( "\nSection?" );
                section =  Integer.parseInt( in.readLine() );

                stmt.executeUpdate( "delete from COURSE where COURSEKEY = '" + courseKey + "' and SECTION = '" + section + "'" );
                conn.commit();
                break;

            case 5:
                whichTable();
                return false;


            case 6:
                return false;

        }

        return true;
    }//end menuCourse

    private boolean menuSchedule() throws Exception {

        String statement;
        String statement2;
        String courseKey;
        int section;
        String roomID;
        String period;
        String time;
        String weekday;
        String name;
        Statement ds = conn.createStatement();
        ResultSet room=null;


        System.out.println( "\nNivel de aislamiento = " + conn.getTransactionIsolation() + "\n" );
        System.out.println( "(1) Lista completa\n" );
        System.out.println( "(2) Lista restringida\n" );
        System.out.println( "(3) Insertar un schedule\n" );
        System.out.println( "(4) Borrar un schedule\n" );
        System.out.println( "(5) Modify the programming of a course\n" );
        System.out.println( "(6) Ir a todas las tablas\n" );
        System.out.println( "(7) Salir\n\n" );
        System.out.print( "Option: " );

        switch( Integer.parseInt( "0" + in.readLine() ) ) {

            case 1:	query( "select * from SCHEDULE" );
                break;

            case 2:	System.out.println( "\nPredicado?" );
                query( "select * from SCHEDULE where " + in.readLine() );
                break;

            case 3:	statement = "insert into SCHEDULE values ( ";

                System.out.println( "\nRoom ID?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Period?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Course key?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Section?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Weekday?" );
                statement += "'" + in.readLine() + "',";

                System.out.println( "Time? Format: HH:MM:SS" );
                statement += "'" + in.readLine() + "',";
                
                statement += "'" + 60 + "',";

                System.out.println( "Semester?" );
                statement += "'" + in.readLine() + "')";

                try
                {
                    stmt.executeUpdate( statement );
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
                conn.commit();
                break;

            case 4:
                System.out.println( "\nCourse Key?" );
                courseKey = in.readLine();

                stmt.executeUpdate( "DELETE schedule, reservation FROM schedule INNER JOIN reservation ON schedule.ROOMID = reservation.ROOMID WHERE schedule.COURSEKEY = '" + courseKey + "' and schedule.TITLE = reservation.RPERIOD and reservation.DATTIME = concat('0000-01-01 ',schedule.STIME)" );
                conn.commit();

                break;

            case 5:

                System.out.println("Which course programming would you want to modify?");
                courseKey = in.readLine();

                System.out.println("Section?");
                section = Integer.parseInt(in.readLine());

                System.out.println("Period?");
                period = in.readLine();

                System.out.println("Time? Format: hh-mi-ss");
                time = in.readLine();

                System.out.println("Weekday?");
                weekday = in.readLine();


                room = ds.executeQuery("select ROOMID from schedule where COURSEKEY = '" + courseKey + "' and SECTION = '" + section + "'");
                stmt.executeUpdate( "update reservation set RWEEKDAY = '" + weekday + "', DATTIME = '0000-01-01 " + time + "' ,RPERIOD = '" + period + "' where ROOMID = '" + room + "' and DATTIME = '0000-01-01 " + time + "' and RPERIOD = '" + period + "'" );
                stmt.executeUpdate( "update schedule set WEEKDAY = '" + weekday + "', STIME = '" + time + "' ,TITLE = '" + period + "' where COURSEKEY = '" + courseKey + "' and SECTION = '" + section + "'" );
                conn.commit();
                break;
            case 6:
                whichTable();
                return false;


            case 7:
                return false;
        }

        return true;
    }//end menuSchedule

    private static void whichTable() throws Exception
    {

        Transaction transaction = new Transaction();
        System.out.println( "Which table do you want to work with?" );
        System.out.println( "(1) Period\n" );
        System.out.println( "(2) Room\n" );
        System.out.println( "(3) Reservation\n" );
        System.out.println( "(4) Course\n" );
        System.out.println( "(5) Schedule\n" );
        System.out.print( "Option: " );

        switch( Integer.parseInt( "0" + in.readLine() ) )
        {

            case 1:

                while( true )

                    try {
                        if( ! transaction.menuPeriod() )
                            break;

                    } catch( Exception e ) {

                        System.err.println( "failed" );
                        e.printStackTrace( System.err );
                    }
                    transaction.close();

                break;

            case 2:
                while( true )

                try
                {
                    if( ! transaction.menuRoom() )
                        break;

                } catch( Exception e ) {

                    System.err.println( "failed" );
                    e.printStackTrace( System.err );
                }
                transaction.close();

                break;

            case 3:
                while( true )

                    try
                    {
                        if( ! transaction.menuReservation() )
                            break;

                    } catch( Exception e ) {

                        System.err.println( "failed" );
                        e.printStackTrace( System.err );
                    }
                    transaction.close();

                break;

            case 4:
                while( true )

                    try {
                        if( ! transaction.menuCourse() )
                            break;

                    } catch( Exception e ) {

                        System.err.println( "failed" );
                        e.printStackTrace( System.err );
                    }
                    transaction.close();

                break;

            case 5:
                while( true )

                    try {
                        if( ! transaction.menuSchedule() )
                            break;

                    } catch( Exception e ) {

                        System.err.println( "failed" );
                        e.printStackTrace( System.err );
                    }
                    transaction.close();

                break;

        }

    }

    public static void main(String[] arg) throws SQLException, Exception
    {

        if( arg.length != 0 )
        {

            System.err.println( "Use: java TransactionMySQL" );
            System.exit( 1 );
        }//end if

        whichTable();

    }
}
