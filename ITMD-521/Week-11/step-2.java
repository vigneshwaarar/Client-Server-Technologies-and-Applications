import java.sql.*;
import java.util.*;

import java.util.Map.Entry;

public class InsertData{

     public static void main(String[] args) throws  SQLException,ClassNotFoundException {

         Class.forName("com.mysql.jdbc.Driver");
          String host = "jdbc:mysql://localhost/hadoopguide";
          String username = "root";
          String password = "itmd521";
          try{

              Connection conn = DriverManager.getConnection(host, username, password);
              System.out.println("database connected");
              Statement st = conn.createStatement();

              ResultSet rs =  st.executeQuery("select * from hadoopguide.widgets;");
              System.out.println("data retrieved");
              ResultSetMetaData md = rs.getMetaData();
              int colCount = md.getColumnCount();
              ArrayList<ArrayList<Object>> dbData = new ArrayList<ArrayList<Object>>();
              ArrayList<String> colName = new ArrayList<String>();

                String col = "";
                for (int i = 1; i <= colCount; i++)
                {
                    col = md.getColumnName(i);
                    colName.add(col);

                }
                // Add records to the vectors
                while (rs.next())
                {
                    ArrayList<Object> rows = new ArrayList<Object>(colCount);

                    for (int i = 1; i <= colCount; i++)
                    {
                        rows.add(rs.getObject(i));
                    }
                    dbData.add(rows);
                }

                // Close ResultSet
                rs.close();
                st.close();
                System.out.println("now delete data");
                Statement deleteST = conn.createStatement();
                deleteST.executeUpdate("delete from hadoopguide.widgets;");
                System.out.println("data deleted");
                Statement autoST = conn.createStatement();
                autoST.executeUpdate("ALTER TABLE hadoopguide.widgets AUTO_INCREMENT = 1;");
                System.out.println("auto done");
              for(int i=0; i<5000; i++)
              {
                  Random ran = new Random();
                  int n1 = ran.nextInt(3);
                  int n2 = ran.nextInt(3);
                  int n3 = ran.nextInt(3);
                  int n4 = ran.nextInt(3);
                  int n5 = ran.nextInt(3);

                  String newStr = "";
                  if(dbData.get(n5).get(5) != null)
                      newStr = "Insert into hadoopguide.widgets values(NULL,'"+ dbData.get(n1).get(1).toString()
                      + "',"+ dbData.get(n2).get(2).toString() + ",'"+ dbData.get(n3).get(3).toString()
                      + "',"+ dbData.get(n4).get(4).toString() + ",'"+ dbData.get(n5).get(5).toString() + "')";
                  else
                      newStr = "Insert into hadoopguide.widgets values(NULL,'"+ dbData.get(n1).get(1).toString()
                      + "'," + dbData.get(n2).get(2).toString() + ",'"+ dbData.get(n3).get(3).toString()
                      + "',"+ dbData.get(n4).get(4).toString() + ", NULL)";
                  Statement insertST = conn.createStatement();
                  insertST.executeUpdate(newStr);
                  insertST.close();
              }
              conn.close();
              System.out.println("Execution done");
          }
          catch(SQLException e){
              System.out.println("statement execution error");
              e.printStackTrace();
          }



     }
}

