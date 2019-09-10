SELECT sMonth,
       Profession,
       mVol,
       sum(mVol)
           OVER (PARTITION BY Profession ORDER BY sMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) total
FROM (SELECT sMonth,
             Profession,
             sum(vol) mVol
      FROM (SELECT YEAR(DATE) * 100 + MONTH(DATE) sMonth, Price * Quantity vol, Profession
            FROM checks
                   JOIN cards ON cards.CardNumber = checks.CardNumber) sales
      GROUP BY sMonth, Profession) mSales
ORDER BY sMonth, Profession


SELECT sMonth,
       Profession,
       sum(sum(vol))
           OVER (PARTITION BY Profession ORDER BY sMonth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) total
FROM (SELECT YEAR(DATE) * 100 + MONTH(DATE) sMonth, Price * Quantity vol, Profession
      FROM checks
             JOIN cards ON cards.CardNumber = checks.CardNumber) sales
GROUP BY sMonth, Profession
ORDER BY sMonth, Profession

SELECT Profession, SUM(Price * Quantity) pVol
FROM checks
       JOIN cards ON cards.CardNumber = checks.CardNumber
GROUP BY Profession
