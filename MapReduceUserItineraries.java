// Copyright: Group 29
//        ID: 1928620 1928512 1927932

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;


public class MapReduceUserItineraries {


    // Create Table Schema in HBase
    public static void createSchemaTables(Configuration config, String table_name, String CF) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            // Table
            TableName table = TableName.valueOf(table_name);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(CF));
            ColumnFamilyDescriptor cfDes = columnFamilyDescriptorBuilder.build();

            tableDescriptorBuilder.setColumnFamily(cfDes);
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

            System.out.print("Creating table: " + table_name);
            // If exists, disable and delete it
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
            admin.createTable(tableDescriptor);
            System.out.println(" Done.");
        }
    }


    // Job1 Mapper: parse CSV files
    // input: CSV File
    // output: <LAC:CELL, city_name>
    public static class CSVMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws InterruptedException, IOException {
            String[] strList = value.toString().split(",");
            if (!key.toString().equals("0")) {
                if (strList.length > 12) {
                    String lac = strList[2];
                    String cell = strList[3];
                    String city_name = (strList[12]);
                    context.write(new Text(lac + ":" + cell), new Text(city_name));
                }
            }
        }
    }

    // Job1 Reducer : write csv into HBase
    // input: <LAC:CELL, city_name>
    // output: Table "CSV"
    public static class CSVHBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public static final byte[] CF = "Cities".getBytes();
        public static final byte[] QUALIFIER1 = "City_name".getBytes();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Store all the location names for each mobile number
            String city_name = null;
            for (Text city : values) {
                city_name = city.toString();
            }

            if (city_name != null) {
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(CF, QUALIFIER1, Bytes.toBytes(city_name));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
            }
        }
    }


    // Job2 Mapper: parse log files
    // input: log files
    // output: <phone_number, LAC:CELL>
    public static class LogsMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text cell = new Text();
        private Text lac = new Text();
        private Text phone_number = new Text();

        public void map(Object key, Text value, Context context
        ) throws InterruptedException, IOException {
            List<String> identifier = new ArrayList<>();
            String[] strList = value.toString().split(",");
            if (strList.length > 4) {
                lac.set(strList[1]);
                cell.set(strList[2]);
                phone_number.set(strList[3]);
                identifier.add(lac.toString());
                identifier.add(cell.toString());
                context.write(phone_number, new Text(String.join(":", identifier)));
            }
        }
    }

    // Job2 Reduce: write result to HBase
    // input: <phone_number, LAC:CELL>
    // output: Table "UserItinerariesHadoop"
    public static class LogsHBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        private Configuration config;
        private Table table;
        public static final byte[] CF = "User".getBytes();
        public static final byte[] QUALIFIER1 = "Itineraries".getBytes();

        // Initializing connection toHBase Table "CSV"
        @Override
        protected void setup(Context context) {
            config = HBaseConfiguration.create();
            try {
                Connection connection = ConnectionFactory.createConnection(config);
                table = connection.getTable(TableName.valueOf("CSV"));
            }  catch (IOException e) {
                System.out.println("Error getting table from HBase"+e);
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Store all the location names for each mobile number
            HashSet<String> locationHashSet = new HashSet<>();

            for (Text lac_cell : values) {
                Get get = new Get(Bytes.toBytes(lac_cell.toString()));
                Result set = table.get(get);
                byte [] value = set.getValue(Bytes.toBytes("Cities"),Bytes.toBytes("City_name"));
                String city = Bytes.toString(value);
                locationHashSet.add(city);
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(CF, QUALIFIER1, Bytes.toBytes(String.join(",", locationHashSet)));

            //write to HBase
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    // Main
    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.err.println("You need to specify the input(csv and logs) path in HDFS" +
                    "{ args[0]: csv_path, args[1]: logs_path }");
            System.exit(2);
        }

        // HBase Config
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

        // Create Table
        createSchemaTables(config, "CSV", "Cities");
        createSchemaTables(config, "UserItinerariesHadoop", "User");

        Configuration conf = new Configuration();

        // Job1 Config
        conf.set(TableOutputFormat.OUTPUT_TABLE, "CSV");
        Job job1 = Job.getInstance(conf, "WriteCSVtoHBase");
        job1.setJarByClass(MapReduceUserItineraries.class);
        job1.setMapperClass(CSVMapper.class);
        job1.setReducerClass(CSVHBaseReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));

        // Add job1 to ControlledJob
        ControlledJob ctrlJob1 = new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        // Job2 Config
        conf.set(TableOutputFormat.OUTPUT_TABLE, "UserItinerariesHadoop");
        Job job2 = Job.getInstance(conf,"WriteResulttoHBase");
        job2.setJarByClass(MapReduceUserItineraries.class);
        job2.setMapperClass(LogsMapper.class);
        job2.setReducerClass(LogsHBaseReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));

        // Add job2 to Controlled job
        ControlledJob ctrlJob2 = new ControlledJob(conf);
        ctrlJob2.setJob(job2);

        // Initializing JobControl
        JobControl jobCtrl = new JobControl("Boss");
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);

        Thread thread = new Thread(jobCtrl);
        thread.start();
        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println("Job Success: "+jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
    }
}