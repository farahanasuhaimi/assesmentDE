SELECT 
    CustomerName,
    Month,
    AVG(TotalSalesAmount) OVER (PARTITION BY CustomerName ORDER BY Month ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MovingAverage
FROM monthlysales m
ORDER BY 
    Month, CustomerName;
