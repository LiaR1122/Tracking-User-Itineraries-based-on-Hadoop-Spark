// Copyright: Individual Challenge Work of DTS205TC
//        ID: 1928620

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.awt.*;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import javax.swing.*;
import java.awt.event.ActionEvent;
import java.nio.charset.StandardCharsets;


public class QueryItinerariesInterface extends JFrame implements ActionListener {
    // Initialization
    JLabel lb1, lb2, lb3, lb4;
    JTextField tf1, tf2, tf3;
    JTextArea tf4;
    JButton btn;

    // Initializing JFrame components
    QueryItinerariesInterface() {

        // Title
        super("User Itineraries");

        // Enter Phone Number:
        lb1 = new JLabel("Enter Phone Number:");
        lb1.setBounds(50, 10, 180, 20);
        tf1 = new JTextField(20);
        tf1.setBounds(220, 10, 180, 20);

        // "Start Date"
        lb2 = new JLabel("Start Date");
        lb2.setBounds(50, 48, 180, 20);
        tf2 = new JTextField(20);
        tf2.setBounds(140, 50, 80, 20);

        // "End Date"
        lb3 = new JLabel("End Date");
        lb3.setBounds(240, 48, 180, 20);
        tf3 = new JTextField(20);
        tf3.setBounds(320, 50, 80, 20);

        // "Location Visited: "
        lb4 = new JLabel("Location Visited: ");
        lb4.setBounds(50, 90, 180, 20);
        tf4 = new JTextArea();
        tf4.setBounds(220, 95, 180, 50);
        tf4.setEditable(false);
        tf4.setLineWrap(true);
        tf4.setWrapStyleWord(true);

        // Button "Query"
        btn = new JButton("Query");
        btn.setBounds(50, 180, 100, 20);
        btn.setLayout(new FlowLayout());
        btn.addActionListener(this);

        // Window Config
        setLocationRelativeTo(null);
        setVisible(true);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(500, 300);
        setLayout(null);

        //Add components to the JFrame
        add(lb1);
        add(lb2);
        add(lb3);
        add(lb4);
        add(tf1);
        add(tf2);
        add(tf3);
        add(tf4);
        add(btn);
    }

    public void actionPerformed(ActionEvent e) {
        //Create DataBase Coonection and Fetching Records
        try {
            // Check input format with regex
            String phone_number = tf1.getText().trim();
            if (!phone_number.matches("(\\d{11})"))
                JOptionPane.showMessageDialog(null, "Please Input a correct Phone Number with 13 digits");

            String start_date = tf2.getText().trim();
            if (!start_date.matches("^\\d{4}-\\d{2}-\\d{2}$"))
                JOptionPane.showMessageDialog(null, "Please Input a correct Start Date (E.g. 2022-01-01)");

            String end_date = tf3.getText().trim();
            if (!end_date.matches("^\\d{4}-\\d{2}-\\d{2}$"))
                JOptionPane.showMessageDialog(null, "Please Input a correct End Date (E.g. 2022-01-01)");

            // Connect to HBase
            Configuration conf = HBaseConfiguration.create();
            conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
            conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf("UserTimeItinerary"));

            // Search for results
            Scan scan = new Scan().withStartRow((phone_number + "_" + start_date).getBytes(StandardCharsets.UTF_8),true)
                    .withStopRow((phone_number + "_" + end_date).getBytes(StandardCharsets.UTF_8),true);

            ResultScanner rscanner = table.getScanner(scan);
            HashSet<String> cityMap = new HashSet<>();
            for (Result result : rscanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String city = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    cityMap.add(city);
                }
            }
            if (!cityMap.isEmpty())
                tf4.setText(String.join(",", cityMap));
            else
                JOptionPane.showMessageDialog(null, "Itineraries not Found");
        } catch (IOException exception) {
            exception.printStackTrace();
        }

    }

    //Running Constructor
    public static void main(String args[]) {
        new QueryItinerariesInterface();
    }
}