// Cypher Queries to load SAP Sample data from GitHub

//Cleanup - To start with a clean slate
MATCH(r:Region) DETACH DELETE r;
MATCH(c:Country) DETACH DELETE c;
MATCH(a:Address) DETACH DELETE a;
MATCH(p:Product) detach DELETE p;
MATCH(pc:ProductCategory) DETACH DELETE pc;
MATCH(bp:BusinessPartner) DETACH DELETE bp;
MATCH(e:Employee) DETACH DELETE e;
MATCH(so:SalesOrder) DETACH DELETE so;

//Region
CREATE CONSTRAINT cnstrnt_unique_rgn IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS NODE KEY;
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Addresses.csv' AS line MERGE (r:Region {name: line.REGION});

//Country
CREATE CONSTRAINT cnstrnt_unique_cntry IF NOT EXISTS
FOR (c:Country) REQUIRE c.country_cd IS NODE KEY;
CREATE INDEX indx_cntry_name IF NOT EXISTS FOR (c:Country) ON (c.country_cd);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Addresses.csv' AS line
WITH line,
CASE line.COUNTRY
WHEN 'US' THEN 'United States of America'
WHEN 'DE' THEN 'Germany'
WHEN 'GB' THEN 'United Kingdom of Great Britain'
WHEN 'AU' THEN 'Australia'
WHEN 'IN' THEN 'India'
WHEN 'DU' THEN 'United Arab Emirates'
WHEN 'CA' THEN 'Canada'
WHEN 'FR' THEN 'France'
END AS country_name
MERGE (c:Country {country_cd: line.COUNTRY})
SET c.name=country_name;

//Address
CREATE CONSTRAINT cnstrnt_unique_addr IF NOT EXISTS
FOR (c:Address) REQUIRE c.address_id IS NODE KEY;

CREATE INDEX indx_addr_name IF NOT EXISTS FOR (a:Address) ON (a.city);
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Addresses.csv' AS line
WITH line,
CASE line.ADDRESSTYPE
WHEN '1' THEN 'Residential'
WHEN '2' THEN 'Commercial'
END AS address_type
MERGE (a:Address {address_id: line.ADDRESSID})
SET a.building=line.BUILDING,
a.address_type=address_type,
a.street=line.STREET,
a.city=line.CITY,
a.postal_code=line.POSTALCODE,
a.country=line.COUNTRY;

//Create Relationships - Region-Country-Address
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Addresses.csv' AS line
MATCH (r:Region {name: line.REGION})
MATCH (c:Country {country_cd: line.COUNTRY})
MATCH (a:Address {address_id: line.ADDRESSID})
MERGE (r)-[:REGION_TO_COUNTRY]->(c)
MERGE (c)-[:COUNTRY_TO_ADDRESS]->(a);

//Product:
CREATE CONSTRAINT cnstrnt_unique_prod IF NOT EXISTS
FOR (p:Product) REQUIRE p.product_id IS NODE KEY;

CREATE INDEX indx_prd_name IF NOT EXISTS FOR (p:Product) ON (p.name);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Products.csv' AS line
MERGE (p:Product{product_id:line.PRODUCTID})
SET
p.quantity_unit=line.QUANTITYUNIT,
p.weight_measure=toInteger(line.WEIGHTMEASURE),
p.weight_unit=line.WEIGHTUNIT,
p.currency=line.CURRENCY,
p.price=toInteger(line.PRICE);

//Update Product Name
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/ProductTexts.csv' AS line
MATCH (p:Product {product_id:line.PRODUCTID})
SET p.name=line.SHORT_DESCR;

//Product Category:
CREATE CONSTRAINT cnstrnt_unique_prod_cat IF NOT EXISTS
FOR (pc:ProductCategory) REQUIRE pc.product_category_id IS NODE KEY;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/ProductCategories.csv' AS line
MERGE (pc:ProductCategory{product_category_id:line.PRODCATEGORYID});

//Update Product Category Name
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/ProductCategoryText.csv' AS line
MATCH (pc:ProductCategory {product_category_id:line.PRODCATEGORYID})
SET pc.name=line.SHORT_DESCR;

// Create Product_Category relationship
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Products.csv' AS line
MATCH (p:Product {product_id:line.PRODUCTID })
MATCH (pc:ProductCategory {product_category_id:line.PRODCATEGORYID})
MERGE (pc)-[:PRODUCT_CATEGORY]->(p);

//Business Partner
CREATE CONSTRAINT cnstrnt_unique_bus_prtnr IF NOT EXISTS
FOR (bp:BusinessPartner) REQUIRE bp.business_partner_id IS NODE KEY;

CREATE INDEX indx_bp_name IF NOT EXISTS FOR (bp:BusinessPartner) ON (bp.company_name);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/BusinessPartners.csv' AS line
MERGE (bp:BusinessPartner{business_partner_id:line.PARTNERID})
SET bp.partner_role=line.PARTNERROLE,
bp.email=line.EMAILADDRESS,
bp.phone=toInteger(line.PHONENUMBER),
bp.fax=toInteger(line.FAXNUMBER),
bp.web=line.WEBADDRESS,
bp.company_name=line.COMPANYNAME,
bp.legalForm=line.LEGALFORM;

//Create 'Located At' relationship
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/BusinessPartners.csv' AS line
MATCH (bp:BusinessPartner {business_partner_id: line.PARTNERID })
MATCH (a:Address {address_id: line.ADDRESSID})
MERGE (bp)-[la:LOCATED_AT]->(a)
SET la.created_at = date(line.CREATEDAT),
la.updated_at = date(line.CHANGEDAT);

//Employee
CREATE CONSTRAINT cnstrnt_unique_emp IF NOT EXISTS
FOR (e:Employee) REQUIRE e.employee_id IS NODE KEY;

CREATE INDEX indx_emp_name IF NOT EXISTS FOR (e:Employee) ON (e.name);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Employees.csv' AS line
MERGE (e:Employee{employee_id:line.EMPLOYEEID})
SET
e.name=line.NAME_FIRST+' '+COALESCE(line.NAME_MIDDLE+' ', '')+line.NAME_LAST,
e.name_initials=line.NAME_INITIALS,
e.gender=line.SEX,
e.language=line.LANGUAGE,
e.phone=line.PHONENUMBER,
e.email=line.EMAILADDRESS,
e.login_name=line.LOGINNAME;

//Optionally fix non utf-8 character 
MATCH (e:Employee {name:"Haseena�al Yousuf"})
SET e.name="Haseena al Yousuf";
MATCH (e:Employee {name:"P�n�lope G Duperr�"})
SET e.name="Pénélope G Duperré";

//Create Employee to Address Relationship
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/Employees.csv' AS line
MATCH (e:Employee {employee_id:line.EMPLOYEEID})
MATCH (a:Address {address_id:line.ADDRESSID})
MERGE (e)-[:RESIDES_AT]->(a);

//SalesOrder
CREATE CONSTRAINT cnstrnt_unique_sls_ordr IF NOT EXISTS
FOR (so:SalesOrder) REQUIRE so.sales_order_id IS NODE KEY;

CREATE INDEX indx_sls_ord_id IF NOT EXISTS FOR (so:SalesOrder) ON (so.sales_order_id);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/SalesOrders.csv' AS line
MERGE (so:SalesOrder{sales_order_id:line.SALESORDERID})
SET so.sales_org=line.SALESORG,
so.currency=line.CURRENCY,
so.gross_amount=toInteger(line.GROSSAMOUNT),
so.net_amount=toInteger(line.NETAMOUNT),
so.tax_amount=toInteger(line.TAXAMOUNT);

//Create Rel Business Partner to Sales Order
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/SalesOrders.csv' AS line
MATCH (so:SalesOrder {sales_order_id:line.SALESORDERID})
MATCH (bp:BusinessPartner {business_partner_id:line.PARTNERID})
MATCH (e:Employee {employee_id:line.CREATEDBY})
MERGE (so)-[pb:PROCESSED_BY]->(e)
SET pb.created_at = date(line.CREATEDAT),
pb.updated_at = date(line.CHANGEDAT)
MERGE (so)-[:PROCESSED_AT]->(bp);

//Create Rel SalesOrder to Product
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/SAP-samples/data-warehouse-cloud-content/main/Sample_Bikes_Sales_content/CSV/SalesOrderItems.csv' AS line
MATCH (so:SalesOrder {sales_order_id:line.SALESORDERID})
MATCH (p:Product {product_id:line.PRODUCTID })
MERGE (so)-[ps:ITEM{sales_order_id:line.SALESORDERID}]->(p)
SET ps.currency=line.CURRENCY,
ps.gross_amount=toInteger(line.GROSSAMOUNT),
ps.net_amount=toInteger(line.NETAMOUNT),
ps.tax_amount=toInteger(line.TAXAMOUNT),
ps.quantity=toInteger(line.QUANTITY),
ps.quantity_unit=line.QUANTITYUNIT,
ps.delivery_date=date(line.DELIVERYDATE);

//Data Analysis Queries

//1. Sales by Region
MATCH (so:SalesOrder)
RETURN so.sales_org as Region, sum(so.gross_amount) as `Total_Regional_Sales(USD)`
ORDER BY `Total_Regional_Sales(USD)` DESC LIMIT 10;

//2. Sales by Employee
MATCH (so:SalesOrder)-[:PROCESSED_BY]->(e:Employee)
RETURN e.name as EmpName, sum(so.gross_amount) as `Total_Sales_By_Emp(USD)`
ORDER BY `Total_Sales_By_Emp(USD)` DESC LIMIT 10;

//3. Sales By Country
MATCH (so:SalesOrder)-[:PROCESSED_AT]->(bp:BusinessPartner)-[:LOCATED_AT]->(a:Address)<-[:COUNTRY_TO_ADDRESS]-(c:Country)
RETURN c.name as Country, sum(so.gross_amount) as `Total_Sales_By_Country(USD)`
ORDER BY `Total_Sales_By_Country(USD)` Desc LIMIT 5;

//4. Top 10 Products
MATCH (so:SalesOrder)-[i:ITEM]->(p:Product)
RETURN p.name as Product, sum(i.quantity) as Quantity
ORDER BY Quantity Desc LIMIT 10;

//5. Top 5 Product Category
MATCH (so:SalesOrder)-[i:ITEM]->(p:Product)<-[:PRODUCT_CATEGORY]-(pc:ProductCategory)
RETURN pc.name as ProductCategory, sum(i.quantity) as Quantity
ORDER BY Quantity Desc LIMIT 5;
