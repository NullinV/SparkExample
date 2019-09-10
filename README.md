Nested XML handling with Spark.

Month volume is calculated by SQL group aggregate function.
Sql window aggregate function is applied to group aggregate to get monthly accumulative total in the same select statement.

        spark.sql("SELECT sMonth, " +
                "       Profession, " +
                "       sum(Vol) mVol," +
                "       sum(sum(Vol)) " +
                "           OVER (PARTITION BY Profession ORDER BY sMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) total " +
                "FROM (SELECT YEAR(DATE) * 100 + MONTH(DATE) sMonth, Price * Quantity vol, Profession " +
                "      FROM checks " +
                "             JOIN cards ON cards.CardNumber = checks.CardNumber) sales " +
                "GROUP BY sMonth, Profession " +
                "ORDER BY sMonth, Profession").show();