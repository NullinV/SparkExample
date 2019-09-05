package base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import org.apache.commons.cli.*;

import static java.lang.System.exit;


public class CardsChecksHandler {

    private static StructType cardScheme = new StructType(new StructField[]{
            new StructField("Age", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("CardNumber", DataTypes.StringType, true, Metadata.empty()),
            new StructField("DateOfBirthday", DataTypes.DateType, true, Metadata.empty()),
            new StructField("FirstName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("LastName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("Profession", DataTypes.StringType, true, Metadata.empty()),
    });

    private static StructType checkScheme = new StructType(new StructField[]{
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

        options.addOption(Option.builder("m").longOpt("master")
                .desc("The Spark master URL")
                .hasArg(true)
                .argName("master URL")
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

        if (!(cmdLine.hasOption("card") || cmdLine.hasOption("check"))) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("capp", options, true);
            exit(0);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Cards & checks data handler")
                .master(cmdLine.getOptionValue("master", "local"))
                .enableHiveSupport()
                .getOrCreate();

        if (cmdLine.hasOption("database")) dataBase = cmdLine.getOptionValue("d");
        spark.sql("CREATE DATABASE IF NOT EXISTS " + dataBase);
        spark.sql("USE " + dataBase);

        if (cmdLine.hasOption("card")) {
            Dataset<Row> df = spark.read()
                    .format("xml")
                    .option("rowTag", "Card")
                    .schema(cardScheme)
                    .load(cmdLine.getOptionValue("card"));

            spark.sql("DROP TABLE IF EXISTS cards");
            df.write().format("ORC").saveAsTable("cards");
        }

        if (cmdLine.hasOption("check")) {
            Dataset<Row> df = spark.read()
                    .format("xml")
                    .option("rowTag", "Check")
                    .schema(checkScheme)
                    .load(cmdLine.getOptionValue("check"));

            Dataset<Row> exploded = df.withColumn("Product", org.apache.spark.sql.functions.explode(df.col("Products.Product"))).drop("Products");

            Dataset<Row> fin = exploded.withColumn("Name", exploded.col("Product.Name"))
                    .withColumn("Price", exploded.col("Product.Price"))
                    .withColumn("Quantity", exploded.col("Product.Quantity"))
                    .drop("Product");

            spark.sql("DROP TABLE IF EXISTS checks");
            fin.write().format("ORC").saveAsTable("checks");

            spark.sql("DROP TABLE IF EXISTS lastchecks");

            spark.sql("SELECT DISTINCT CardNumber, " +
                    "LAST_VALUE(Date) OVER (PARTITION BY CardNumber, Name ORDER BY Date ROWS between UNBOUNDED PRECEDING and UNBOUNDED following) Date," +
                    "Name, " +
                    "LAST_VALUE(Price) OVER (PARTITION BY CardNumber, Name  ORDER BY Date ROWS between UNBOUNDED PRECEDING and  UNBOUNDED following) Price," +
                    "LAST_VALUE(Quantity) OVER (PARTITION BY CardNumber, Name  ORDER BY Date ROWS between UNBOUNDED PRECEDING and UNBOUNDED following) Quantity FROM CHECKS")
                    .write().format("ORC").saveAsTable("lastchecks");
        }
    }

    public static void main(String[] args) {
        CardsChecksHandler handler = new CardsChecksHandler();
        handler.perform(args);
        SparkSession spark = SparkSession
                .builder()
                .appName("Main task")
                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        System.out.println("Tables");
        spark.sql("SHOW TABLES").show();
        System.out.println("Cards");
        spark.sql("select * from cards").show();
        System.out.println("All sales");
        spark.sql("select * from checks").show();
        System.out.println("Last sale for each card-product pair");
        spark.sql("select * from lastchecks").show();

        System.out.println("Total sales by profession");
        spark.sql("SELECT Profession, sum(Price*Quantity) pVol from checks join cards on cards.CardNumber = checks.CardNumber group by Profession").show();

        System.out.println("Monthly");
        spark.sql("select sMonth,Profession, sum(vol) mVol from (select concat(year(Date), '-', right(concat('0',month(date)),2)) sMonth, Price*Quantity vol,  Profession from checks join cards on cards.CardNumber = checks.CardNumber) sales group by sMonth,Profession order by sMonth, Profession").show();

        System.out.println("Running total v1");
        spark.sql("SELECT sMonth, " +
                "       Profession, " +
                "       mVol, " +
                "       sum(mVol) " +
                "           OVER (PARTITION BY Profession ORDER BY sMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) total " +
                "FROM (SELECT sMonth, " +
                "             Profession, " +
                "             sum(vol) mVol" +
                "      FROM (SELECT YEAR(DATE) * 100 + MONTH(DATE) sMonth, Price * Quantity vol, Profession " +
                "            FROM checks " +
                "                   JOIN cards ON cards.CardNumber = checks.CardNumber) sales " +
                "      GROUP BY sMonth, Profession) mSales " +
                "ORDER BY sMonth, Profession").show();

        System.out.println("Running total v2");
        spark.sql("SELECT sMonth, " +
                "       Profession, " +
                "       sum(sum(Vol)) " +
                "           OVER (PARTITION BY Profession ORDER BY sMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) total " +
                "FROM (SELECT YEAR(DATE) * 100 + MONTH(DATE) sMonth, Price * Quantity vol, Profession " +
                "      FROM checks " +
                "             JOIN cards ON cards.CardNumber = checks.CardNumber) sales " +
                "GROUP BY sMonth, Profession " +
                "ORDER BY sMonth, Profession").show();
    }
}

