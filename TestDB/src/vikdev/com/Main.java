package vikdev.com;



import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {
    public static final String DB_NAME = "testJava.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:E:\\Java\\Database\\TestDB\\" + DB_NAME;
    public static final String TABLE_CONTACTS = "contacts";

    public static final String COLUMN_NAME = "name";

    public static final String COLUMN_PHONE = "phone";

    public static final String COLUMN_EMAIL = "email";

    public static void main(String[] args) {
        try(Connection conn = DriverManager.getConnection(CONNECTION_STRING);
            Statement statement = conn.createStatement();
        ){
            statement.execute("DROP TABLE IF EXISTS " + TABLE_CONTACTS);

            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_CONTACTS +
                    "(" +COLUMN_NAME + " TEXT, " +
                        COLUMN_PHONE + " phone, "+
                        COLUMN_EMAIL + " email"+ ")");

            insertContact(statement,"vikas",12345,"vikas@gmail.com");

            insertContact(statement,"ansh",24232,"ansh@gmail.com");

            insertContact(statement,"shubham",54645,"shubham@gmail.com");

            insertContact(statement,"sarthak",28299,"sarthak@gmail.com");


            statement.execute("UPDATE " + TABLE_CONTACTS + " SET " + COLUMN_PHONE + " = 965552 " + " WHERE " + COLUMN_NAME + "= 'sarthak' ");

            statement.execute("DELETE FROM " + TABLE_CONTACTS + " WHERE " + COLUMN_NAME + " = 'shubham'");

            ResultSet result = statement.executeQuery("SELECT * FROM contacts");
            while (result.next()){
                System.out.println(result.getString("name") + " : "+
                                    result.getInt("phone") + " : " +
                                    result.getString("email"));
            }
            result.close();

        }catch(SQLException e){
            System.out.println("Something went wrong : " + e.getMessage());
            e.printStackTrace();
        }

    }

    public static void insertContact(Statement statement,String name, int phone, String email) throws SQLException{
        statement.execute("INSERT INTO " + TABLE_CONTACTS +
                "(" + COLUMN_NAME + ", " + COLUMN_PHONE + " , " + COLUMN_EMAIL + ")" +
                "VALUES( '" + name + "' , " + phone + " , '" + email +"' )");

    }
}
