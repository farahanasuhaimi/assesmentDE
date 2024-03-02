SELECT 
    c.Name AS CustomerName,
    EXTRACT(MONTH FROM s.SaleDate) AS Month,
    SUM(s.Amount) AS TotalSalesAmount
FROM 
    customers c
JOIN 
    sales s ON c.CustomerID = s.CustomerID
GROUP BY 
    c.Name, EXTRACT(MONTH FROM s.SaleDate)
ORDER BY 
    EXTRACT(MONTH FROM s.SaleDate), c.Name;
