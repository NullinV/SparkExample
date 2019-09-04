package base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import org.apache.commons.cli.*;

import static java.lang.System.exit;


public class CardsChecksHandler {

    private static StructType cardSchema = new StructType(new StructField[]{
            new StructField("Age", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("CardNumber", DataTypes.StringType, true, Metadata.empty()),
            new StructField("DateOfBirthday", DataTypes.DateType, true, Metadata.empty()),
            new StructField("FirstName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("LastName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("Profession", DataTypes.StringType, true, Metadata.empty()),
    });

    private static StructType checkSchema = new StructType(new StructField[]{
            new StructField("CardNumber", DataTypes.StringType, true, Metadata.empty()),
            new StructField("Date", DataTypes.DateType, true, Metadata.empty()),
            new StructField("Products", new StructType(new StructField[]{new StructField("Product", DataTypes.createArrayType(
                    new StructType(new StructField[]{
                            new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
                            new StructField("Price", DataTypes.DoubleType, true, Metadata.empty()),
                            new StructField("Quantity", DataTypes.DoubleType, true, Metadata.empty())
                    })), true, Metadata.empty())}), true, Metadata.empty())}
    );

    private static Options options = new Options();

    static {
        options.addOption(Option.builder("d").longOpt("database")
                .desc("The DB name")
                .hasArg(true)
                .argName("DB_name")
                .required(false)
                .build());

        options.addOption(Option.builder("c").longOpt("card")
                .desc("The issued cards data file")
                .hasArg(true)
                .argName("cards_file")
                .required(false)
                .build());

        options.addOption(Option.builder("h").longOpt("check")
                .desc("The checks data file")
                .hasArg(true)
                .argName("checks_file")
                .required(false)
                .build());
    }

    private CommandLine cmdLine;
    private String dataBase = "ccbase";

    private CommandLine parseCommandLine(Options options, String[] args) {

        CommandLineParser cmdLineParser = new DefaultParser();
        try {
            cmdLine = cmdLineParser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            exit(1);
        }
        return cmdLine;
    }

    public void perform(String[] args) {
        cmdLine = parseCommandLine(options, args);
        String appName = "Cards & checks data handler";

        if (!(cmdLine.hasOption("card") || cmdLine.hasOption("check"))) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("capp", options, true);
            exit(0);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        if (cmdLine.hasOption("database")) dataBase = cmdLine.getOptionValue("d");
        spark.sql("CREATE DATABASE IF NOT EXISTS " + dataBase);
        spark.sql("USE " + dataBase);

        spark.sql("SHOW TABLES").show();

        if (cmdLine.hasOption("card")) {
            Dataset<Row> df = spark.read()
                    .format("xml")
                    .option("rowTag", "Card")
                    .schema(cardSchema)
                    .load(cmdLine.getOptionValue("card")).toDF();

            spark.sql("DROP TABLE IF EXISTS cards");
            df.write().format("ORC").saveAsTable("cards");
        }

        if (cmdLine.hasOption("check")) {
            Dataset<Row> df = spark.read()
                    .format("xml")
                    .option("rowTag", "Check")
                    .schema(checkSchema)
                    .load(cmdLine.getOptionValue("check")).toDF();

            Dataset<Row> exploded = df.withColumn("Product", org.apache.spark.sql.functions.explode(df.col("Products.Product"))).drop("Products");
            exploded.printSchema();
            Dataset<Row> fin = exploded.withColumn("Name", exploded.col("Product.Name"))
                    .withColumn("Price", exploded.col("Product.Price"))
                    .withColumn("Quantity", exploded.col("Product.Quantity"))
                    .drop("Product");

            spark.sql("DROP TABLE IF EXISTS checks");
            fin.write().format("ORC").saveAsTable("checks");

        }

        spark.sql("SHOW TABLES").show();
        spark.sql("select * from cards").show();
        spark.sql("select * from checks").show();
    }

    public static void main(String[] args) {
        CardsChecksHandler handler = new CardsChecksHandler();
        handler.perform(args);

    }
}

