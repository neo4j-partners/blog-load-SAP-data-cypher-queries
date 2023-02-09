//1. Top Business Partners
MATCH (so:SalesOrder)-[:PROCESSED_AT]->(bp:BusinessPartner)
RETURN bp.company_name as Business_Partner_Name, sum(so.gross_amount) as `Total_Sales_By_Bus_Prtnr (USD)`
ORDER BY `Total_Sales_By_Bus_Prtnr (USD)` DESC LIMIT 10;

//2. Top Employees
MATCH (so:SalesOrder)-[:PROCESSED_BY]->(e:Employee)
RETURN e.name as Emp_Name, sum(so.gross_amount) as `Total_Sales_By_Emp (USD)`
ORDER BY `Total_Sales_By_Emp (USD)` DESC LIMIT 10;

//3. Sales by Region
MATCH (so:SalesOrder)
RETURN so.sales_org as Region, sum(so.gross_amount) as `Total_Regional_Sales (USD)`
ORDER BY `Total_Regional_Sales (USD)` DESC LIMIT 10;

//4. Sales By Country
MATCH (so:SalesOrder)-[:PROCESSED_AT]->(bp:BusinessPartner)-[:LOCATED_AT]->(a:Address)<-[:COUNTRY_TO_ADDRESS]-(c:Country)
RETURN c.name as Country, sum(so.gross_amount) as `Total_Sales_By_Country (USD)`
ORDER BY `Total_Sales_By_Country (USD)` Desc LIMIT 5;

//5. Top 10 Products
MATCH (so:SalesOrder)-[i:ITEM]->(p:Product)
RETURN p.name as Product, sum(i.quantity) as Quantity
ORDER BY Quantity Desc LIMIT 10;

//6. Top 5 Product Category
MATCH (so:SalesOrder)-[i:ITEM]->(p:Product)<-[:PRODUCT_CATEGORY]-(pc:ProductCategory)
RETURN pc.name as Product_Category, sum(i.quantity) as Quantity
ORDER BY Quantity Desc LIMIT 5;
