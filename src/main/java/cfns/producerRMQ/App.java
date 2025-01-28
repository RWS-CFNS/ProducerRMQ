package cfns.producerRMQ;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP;
import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class App {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("config.properties")) {
            properties.load(input);
        } catch (IOException e) {
            System.err.println("Error loading config.properties: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        int limit;
        try {
            limit = Integer.parseInt(properties.getProperty("sql.limit"));
        } catch (NumberFormatException e) {
            System.err.println("Invalid limit value in config.properties: " + e.getMessage());
            return;
        }

        String url = properties.getProperty("db.url");
        String username = properties.getProperty("db.username");
        String password = properties.getProperty("db.password");

        try (java.sql.Connection con = DriverManager.getConnection(url, username, password);
             Statement st = con.createStatement()) {

            boolean sendWeather = Boolean.parseBoolean(properties.getProperty("send.weather"));
            boolean sendConn = Boolean.parseBoolean(properties.getProperty("send.conn"));
            boolean sendAis = Boolean.parseBoolean(properties.getProperty("send.ais"));

            JSONObject jsonTables = new JSONObject();

            if (sendWeather) {
                try {
                    String weatherSql = "SELECT * FROM weather ORDER BY time DESC LIMIT " + limit;
                    ResultSet rs = st.executeQuery(weatherSql);
                    JSONArray weatherArray = new JSONArray();

                    while (rs.next()) {
                        JSONObject jsonRow = new JSONObject();
                        jsonRow.put("id", rs.getInt("id"));
                        jsonRow.put("temp", rs.getFloat("temp"));
                        jsonRow.put("humid", rs.getInt("humid"));
                        jsonRow.put("winddir", rs.getInt("winddir"));
                        jsonRow.put("windspeed", rs.getFloat("windspeed"));
                        jsonRow.put("dauw", rs.getFloat("dauw"));
                        jsonRow.put("druk", rs.getFloat("druk"));
                        jsonRow.put("time", rs.getString("time"));
                        weatherArray.put(jsonRow);
                    }
                    jsonTables.put("weather", weatherArray);
                } catch (SQLException e) {
                    System.err.println("Error querying weather data: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            if (sendConn) {
                try {
                    String connSql = "SELECT id, mno, ST_AsText(coordinates) as coordinates, rsrq, rsrp, sinr, weather_id, tijd, rssi, lat, long, type, cell_plmn, tac_lac, cell_utran_id, lat_mast, long_mast " +
                            "FROM conn " +
                            "ORDER BY tijd DESC " +
                            "LIMIT " + limit;
                    ResultSet rs = st.executeQuery(connSql);
                    JSONArray connArray = new JSONArray();

                    while (rs.next()) {
                        JSONObject jsonRow = new JSONObject();
                        jsonRow.put("id", rs.getInt("id"));
                        jsonRow.put("mno", rs.getString("mno"));
                        jsonRow.put("coordinates", rs.getString("coordinates"));
                        jsonRow.put("rsrq", rs.getFloat("rsrq"));
                        jsonRow.put("rsrp", rs.getFloat("rsrp"));
                        jsonRow.put("sinr", rs.getFloat("sinr"));
                        jsonRow.put("weather_id", rs.getInt("weather_id"));
                        jsonRow.put("tijd", rs.getTimestamp("tijd").toString());
                        jsonRow.put("rssi", rs.getInt("rssi"));
                        jsonRow.put("lat", rs.getFloat("lat"));
                        jsonRow.put("long", rs.getFloat("long"));
                        jsonRow.put("type", rs.getString("type"));
                        jsonRow.put("cell_plmn", rs.getInt("cell_plmn"));
                        jsonRow.put("tac_lac", rs.getInt("tac_lac"));
                        jsonRow.put("cell_utran_id", rs.getInt("cell_utran_id"));
                        jsonRow.put("lat_mast", rs.getFloat("lat_mast"));
                        jsonRow.put("long_mast", rs.getFloat("long_mast"));
                        connArray.put(jsonRow);
                    }
                    jsonTables.put("conn", connArray);
                } catch (SQLException e) {
                    System.err.println("Error querying conn data: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            if (sendAis) {
                try {
                    String aisSql = "SELECT * FROM ais ORDER BY received_at DESC";
                    ResultSet rs = st.executeQuery(aisSql);
                    JSONArray aisArray = new JSONArray();

                    while (rs.next()) {
                        JSONObject jsonRow = new JSONObject();
                        jsonRow.put("id", rs.getInt("id"));
                        jsonRow.put("ais_message", rs.getString("ais_message"));
                        jsonRow.put("received_at", rs.getTimestamp("received_at").toString());
                        aisArray.put(jsonRow);
                    }
                    jsonTables.put("ais", aisArray);
                } catch (SQLException e) {
                    System.err.println("Error querying ais data: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            String message = jsonTables.toString();

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(properties.getProperty("rabbitmq.host"));
            factory.setUsername(properties.getProperty("rabbitmq.username"));
            factory.setPassword(properties.getProperty("rabbitmq.password"));

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);

                AMQP.BasicProperties propertiesRMQ = new AMQP.BasicProperties.Builder()
                        .deliveryMode(2) // 2 = persistent
                        .build();

                channel.basicPublish("", QUEUE_NAME, propertiesRMQ, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");

                System.out.println(
                        "                /_____             \r\n"
                                + "          _____/______|            \r\n"
                                + "  _______/_____\\______\\_____     \r\n"
                                + "  \\     Barend Biesheuvel    |    \r\n"
                                + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ");
            } catch (Exception e) {
                System.err.println("Error with RabbitMQ: " + e.getMessage());
                e.printStackTrace();
            }

        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
//package cfns.producerRMQ;
//
//import com.rabbitmq.client.ConnectionFactory;
//import com.rabbitmq.client.Connection;
//import com.rabbitmq.client.Channel;
//import com.rabbitmq.client.AMQP;
//import org.json.JSONArray;
//import org.json.JSONObject;
//import java.sql.*;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.Properties;
//
//public class App {
//    private final static String QUEUE_NAME = "hello";
//
//    public static void main(String[] argv) throws Exception {
//    	Properties properties = new Properties();
//    	try (InputStream input = new FileInputStream("config.properties")) {
//    	    properties.load(input);
//    	} catch (IOException e) {
//    	    System.out.println("Sorry, unable to find config.properties");
//    	    e.printStackTrace();
//    	    return;
//    	}
//
//        // Get the limit value from properties
//        int limit = Integer.parseInt(properties.getProperty("sql.limit"));
//
//        // PostgreSQL Connection Info
//        String url = properties.getProperty("db.url");
//        String username = properties.getProperty("db.username");
//        String password = properties.getProperty("db.password");
//        java.sql.Connection con = DriverManager.getConnection(url, username, password);
//        Statement st = con.createStatement();
//
//        // Determine which tables to send
//        boolean sendWeather = Boolean.parseBoolean(properties.getProperty("send.weather"));
//        boolean sendConn = Boolean.parseBoolean(properties.getProperty("send.conn"));
//        boolean sendAis = Boolean.parseBoolean(properties.getProperty("send.ais"));
//        
//        JSONObject jsonTables = new JSONObject();
//
//        if (sendWeather) {
//            String weatherSql = "SELECT * FROM weather ORDER BY time DESC LIMIT " + limit;
//            ResultSet rs = st.executeQuery(weatherSql);
//            JSONArray weatherArray = new JSONArray();
//
//            while (rs.next()) {
//                JSONObject jsonRow = new JSONObject();
//                jsonRow.put("id", rs.getInt("id"));
//                jsonRow.put("temp", rs.getFloat("temp"));
//                jsonRow.put("humid", rs.getInt("humid"));
//                jsonRow.put("winddir", rs.getInt("winddir"));
//                jsonRow.put("windspeed", rs.getFloat("windspeed"));
//                jsonRow.put("dauw", rs.getFloat("dauw"));
//                jsonRow.put("druk", rs.getFloat("druk"));
//                jsonRow.put("time", rs.getString("time"));
//                weatherArray.put(jsonRow);
//            }
//            jsonTables.put("weather", weatherArray);
//        }
//
//        if (sendConn) {
//        	String connSql = "SELECT id, mno, ST_AsText(coordinates) as coordinates, rsrq, rsrp, sinr, weather_id, tijd, rssi, lat, long, type, cell_plmn, tac_lac, cell_utran_id, lat_mast, long_mast " +
//                    "FROM conn " +
//                    "ORDER BY tijd DESC " +
//                    "LIMIT " + limit;
//            try (ResultSet rs = st.executeQuery(connSql)) {
//                JSONArray connArray = new JSONArray();
//
//                while (rs.next()) {
//                    JSONObject jsonRow = new JSONObject();
//                    jsonRow.put("id", rs.getInt("id"));
//                    jsonRow.put("mno", rs.getString("mno"));
//                    jsonRow.put("coordinates", rs.getString("coordinates"));
//                    jsonRow.put("rsrq", rs.getFloat("rsrq"));
//                    jsonRow.put("rsrp", rs.getFloat("rsrp"));
//                    jsonRow.put("sinr", rs.getFloat("sinr"));
//                    jsonRow.put("weather_id", rs.getInt("weather_id"));
//                    jsonRow.put("tijd", rs.getTimestamp("tijd").toString());
//                    jsonRow.put("rssi", rs.getInt("rssi"));
//                    jsonRow.put("lat", rs.getFloat("lat"));
//                    jsonRow.put("long", rs.getFloat("long"));
//                    jsonRow.put("type", rs.getString("type"));
//                    jsonRow.put("cell_plmn", rs.getInt("cell_plmn"));
//                    jsonRow.put("tac_lac", rs.getInt("tac_lac"));
//                    jsonRow.put("cell_utran_id", rs.getInt("cell_utran_id"));
//                    jsonRow.put("lat_mast", rs.getFloat("lat_mast"));
//                    jsonRow.put("long_mast", rs.getFloat("long_mast"));
//                    connArray.put(jsonRow);
//                }
//                jsonTables.put("conn", connArray);
//            }
//        }
//        
//        if (sendAis) {
//        	String aisSql = "SELECT * FROM ais ORDER BY received_at DESC";
//            try (ResultSet rs = st.executeQuery(aisSql)) {
//                JSONArray aisArray = new JSONArray();
//
//                while (rs.next()) {
//                    JSONObject jsonRow = new JSONObject();
//                    jsonRow.put("id", rs.getInt("id"));
//                    jsonRow.put("ais_message", rs.getString("ais_message"));
//                    jsonRow.put("received_at", rs.getTimestamp("received_at").toString());
//                    aisArray.put(jsonRow);
//                }
//                jsonTables.put("ais", aisArray);
//            }
//        }
//
//        String message = jsonTables.toString();
//
//        // RabbitMQ Connection
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost(properties.getProperty("rabbitmq.host"));
//        factory.setUsername(properties.getProperty("rabbitmq.username"));
//        factory.setPassword(properties.getProperty("rabbitmq.password"));
//
//        try (Connection connection = factory.newConnection();
//             Channel channel = connection.createChannel()) {
//            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
//
//            AMQP.BasicProperties propertiesRMQ = new AMQP.BasicProperties.Builder()
//                    .deliveryMode(2) // 2 = persistent
//                    .build();
//            
//            channel.basicPublish("", QUEUE_NAME, propertiesRMQ, message.getBytes());
//            System.out.println(" [x] Sent '" + message + "'");
//
//            System.out.println(
//                      "                /_____             \r\n"
//                    + "          _____/______|            \r\n"
//                    + "  _______/_____\\______\\_____     \r\n"
//                    + "  \\     Barend Biesheuvel    |    \r\n"
//                    + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ");
//        }
//    }
//}