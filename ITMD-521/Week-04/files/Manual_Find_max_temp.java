
//import statements
import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;



public class Manual_Find_max_temp{

     public static void main(String[] args) throws  SQLException,ClassNotFoundException {

        Connection connection_new = null;
        BufferedReader buffer_reader_new = null;
        Statement statement_new = null;
        try{
        	Class.forName("com.mysql.jdbc.Driver");
         }
         catch(ClassNotFoundException e){
        System.out.println("'com.mysql.jdbc.Driver' not found. add mysql connector jar file");
         }
        try{
        	connection_new = DriverManager.getConnection("jdbc:mysql://127.0.0.1/itmd521_1", "root", "itmd521");
        	statement_new = connection_new.createStatement();
        	System.out.println("Connection established");
         }catch(SQLException e){
        	 System.out.println("connection can't be made. please check your database name, username and password");
         }
         try {
        	 	String file_data;
        	 	buffer_reader_new = new BufferedReader(new FileReader("9293-sample.txt"));
        	 	while ((file_data = buffer_reader_new.readLine()) != null) {
	             String usa_wsi_id =file_data.substring(4,10);
	             String wban_wsi_id =file_data.substring(4,10);
	             String observation_date =file_data.substring(15,19);
	             String latitude_degree =file_data.substring(28,33);
	             String longitude_degree =file_data.substring(34,40);
	             String elevation_meters =file_data.substring(46,50);
	             String wind_direction =file_data.substring(60,62);
	             String sky_ch_meters =file_data.substring(70,74);
	             String visibility_dist =file_data.substring(78,83);
                 String air_temperature ;
                 if (file_data.charAt(87)=='+'){
                	 air_temperature  = file_data.substring(88, 92);
                 }
                 else {
                	 air_temperature  = file_data.substring(87, 92);
                 }
                 String dew_pt_temperature =file_data.substring(93,97);
                 String atm_pressure =file_data.substring(99,103);
                 String insert_SQL= "insert into itmd521_1.9293_Data values (?,?,?,?,?,?,?,?,?,?,?,?);";
                 PreparedStatement insert_statement = connection_new.prepareStatement(insert_SQL);
                 insert_statement .setString(1,usa_wsi_id );
                 insert_statement .setString(2,wban_wsi_id);
                 insert_statement .setString(3,observation_date);
                 insert_statement .setString(4,latitude_degree );
                 insert_statement .setString(5,longitude_degree);
                 insert_statement .setString(6,elevation_meters );
                 insert_statement .setString(7,wind_direction );
                 insert_statement .setString(8,sky_ch_meters);
                 insert_statement .setString(9,visibility_dist );
                 insert_statement .setString(10,air_temperature );
                 insert_statement .setString(11,dew_pt_temperature);
                 insert_statement .setString(12,atm_pressure);
                 try{
                	 insert_statement .execute();
                  } catch (Exception e) {
					// TODO: handle exception
                	  System.out.println("error while inserting data");
				}
                 }
                 String select_SQL="select observation_date, max(air_temperature) from itmd521_1.9293_Data where air_temperature != 9999 group by observation_date;";
                 PreparedStatement select_statement = connection_new.prepareStatement(select_SQL);
                 System.out.println("---------------------------------------");
                 System.out.println("Max Temparature in years - 1992 & 1993");
                 System.out.println("---------------------------------------");
                 try{
	                 ResultSet results = select_statement.executeQuery();
	                 while (results.next())
	                 {
	                    System.out.println(results.getString(1)+"   "+results.getString(2));
	                 }
                 }  catch (Exception e) {
 					// TODO: handle exception
               	  System.out.println("error while retrieving data");
				}

             } catch (IOException e) {
                   e.printStackTrace();
             }
     	}
     }