// Copyright: Individual Assignment for DTS205TC
// ID: 1928620


import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;

import java.util.*;
import static org.apache.spark.sql.functions.col;


/*   This task aims to predict the province in Chinese Mainland based on the latitude and longitude  */
public class SparkMLProvincePred {
    public static void main(String[] args) {
        HashMap<String,Double> labelMap = new HashMap<>();
        labelMap.put("北京市",0.0);
        labelMap.put("天津市",1.0);
        labelMap.put("上海市",2.0);
        labelMap.put("重庆市",3.0);
        labelMap.put("内蒙古自治区",4.0);
        labelMap.put("广西壮族自治区",5.0);
        labelMap.put("西藏自治区",6.0);
        labelMap.put("宁夏回族自治区",7.0);
        labelMap.put("新疆维吾尔自治区",8.0);
        labelMap.put("河北省",9.0);
        labelMap.put("山西省",10.0);
        labelMap.put("辽宁省",11.0);
        labelMap.put("吉林省",12.0);
        labelMap.put("黑龙江省",13.0);
        labelMap.put("江苏省",14.0);
        labelMap.put("安徽省",15.0);
        labelMap.put("广东省",16.0);
        labelMap.put("四川省",17.0);
        labelMap.put("甘肃省",18.0);
        labelMap.put("福建省",19.0);
        labelMap.put("浙江省",20.0);
        labelMap.put("山东省",21.0);
        labelMap.put("海南省",22.0);
        labelMap.put("湖北省",23.0);
        labelMap.put("江西省",24.0);
        labelMap.put("贵州省",25.0);
        labelMap.put("湖南省",26.0);
        labelMap.put("青海省",27.0);
        labelMap.put("陕西省",28.0);
        labelMap.put("云南省",29.0);
        labelMap.put("河南省",30.0);



        SparkSession session = SparkSession.builder().master("local").getOrCreate();
        if (args.length != 2) {
            System.err.println("Please enter the input file path from the command line");
            System.exit(1);
        }

        Dataset<Row> df = session.read().option("delimiter", ",").option("header", "true").csv(args[0]);
        df = df.withColumn("LNG",df.col("LNG").cast("double"));
        df = df.withColumn("LAT",df.col("LAT").cast("double"));
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"LNG","LAT"})
                .setOutputCol("Coordinate");
        Dataset<Row> output = assembler.transform(df);
        col("REGION");

        output = output.select("REGION","Coordinate")
                .where(col("COUNTRY")
                .equalTo("中国"))
                .where(col("REGION").isNotNull());

        output.show();
        JavaRDD<Row> rowJavaRDD = output.javaRDD();
        JavaRDD<LabeledPoint> labeledPoint = rowJavaRDD.map(row -> {
            String Prov = (String) row.get(0);
            Vector value = row.getAs("Coordinate");
            double label = labelMap.get(Prov);
            return new LabeledPoint(label, Vectors.fromML(value));
        });
        JavaRDD<LabeledPoint>[] splits = labeledPoint.randomSplit(new double[]{0.8, 0.2},42);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];

        // Empty categoricalFeaturesInfo indicates all features are continuous.
        int numClasses = 31;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        int numTrees = 100; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "gini";
        int maxDepth = 5;
        int maxBins = 100;
        int seed = 42;
        RandomForestModel model = RandomForest.trainClassifier(training, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        // Compute raw scores on the test set.
        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));
        predictionAndLabels.foreach(data -> {
            System.out.println("Prediction: "+data._1+" , True: " + data._2);
        });

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix: \n" + confusion);
//        Seq<org.apache.spark.mllib.linalg.Vector> confusion_row = confusion.rowIter().toSeq().;
////        JavaRDD<double[]> rdd_confusion = confusion_row;
//        Dataset<Row> df_confusion = session.sparkContext().parallelize(confusion_row).toDF("Row");
//        System.out.println("Confusion matrix: \n" + confusion);

        // Overall statistics
        System.out.println("Accuracy = " + metrics.accuracy());

    }
}