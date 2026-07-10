

""" Q1. List all bookings made by a person named Darren Smith as the following. """



# Option 1
raw_cust_df= ( spark.read.foramt('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/customers.csv")
              )

#Option 2 
raw_cust_df = spark.read.table("dev.spark_db.customers")



from pyspark.sql.functions import col, expr, count, min, max, sum, avg

cust_df = ( raw_cust_df.groupBy('field1', 'field2')
                     .filter(col('field1').isNotNull() )
                     .agg.expr('count(*) as count')
                     .orderBy(col('count').desc())  
                     .limit(3)
          )



1 I need to 


How:

--CTE1 AS (


--         )
--SELECT YEAR, ORDER_DATE, PREV_PUR_DATE, DATEDIFF(ORDER_DATE, PREV_PUR_DATE) AS DIFF
--FROM CTE1         

)
---------------SQL query----------------

--WITH CTE1 AS ( 
            SELECT m.member_id, m.first_name, m.last_name, m.address, b.facility_id, b.slots
            FROM dev.spark_db.members  as m
            FULL OUTER JOIN dev.spark_db.bookings as b ON m.member_id = b.member_id
            WHERE m.first_name = 'Darren' AND m.last_name = 'Smith' AND b.slots IS NOT NULL
            ORDER BY b.slots DESC NULLS FIRST
--             ),

--SELECT CUSTOMERID, ORDER_DATE, PREV_PUR_DATE, DATEDIFF(ORDER_DATE, PREV_PUR_DATE) AS DIFF
--FROM CTE1 
;

---------------Pyspark----------------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg
#from pYspark.sql.window import Window



result_df =  ( spark.read.table("dev.spark_db.customers")
             )


result_df.display()

---------------Pyspark using TemporaryView----------------
raw_emp_df.createOrReplaceView("employee")

sql_query = """
            SELECT employee_id, name
            FROM employee
            where departmentid IS NULL
            """
result_emp_df = spark.sql(sql_query)






raw_mem_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/members.csv")
              )

raw_mem_df.display()



raw_bok_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/bookings.csv")
              )

raw_bok_df.display()



raw_fac_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/facilities.csv")
              )

raw_fac_df.display()




















-----------------------FIND DUPLICATES ------------------------------
--Q1 How to Find duplicates

SELECT ID, COUNT(*) AS DUP_COUNT
FROM   ACS_SBOX0.HBL_TEST_03_03_CTRY
GROUP  BY ID
HAVING COUNT(*) > 1;

-- Q1 How to find duplicates( using row_number)
SELECT *, ROW_NUMBER() OVER(PARTITION BY ID ORDER BY ID DESC) AS RN
FROM   ACS_SBOX0.HBL_TEST_03_03_CTRY


-----------------------DISTINCT/Filtering out/DELETE DUPLICATES ------------------------------
--Q3. HOW TO REMOVE DUPLICATE ROWS IN SQL

------------ OPTION 1) USING DISTINCT ------------
SELECT DISTINCT * FROM ACS_SBOX0.HBL_TEST_03_03_CTRY;


------------ OPTION 2) Filtering out USING CTE and ROW_NUMBER AND SELECTING 
WITH T1 AS (SELECT T.*, ROW_NUMBER() OVER(PARTITION BY ID ORDER BY ID DESC) AS ROW_N
            FROM   ACS_SBOX0.HBL_TEST_03_03_CTRY T
              )
SELECT * FROM T1              
WHERE  ROW_N =1;
                                                  

------------ OPTION 3)  CHECK THIS CAREFULLY CAUSE IT REMOVES UNDESIRED RECORDS!!!!!
------------ OPTION3 USING CTE, ROW_NUMBER AND DELETING. 
"""
WITH DuplicateCTE AS (SELECT *,
                              ROW_NUMBER() OVER ( PARTITION BY column1, column2, column3 -- Define what makes a row a duplicate
                                                  ORDER BY some_unique_id_column -- Keep the one with the lowest/highest ID
                                                 ) as row_num
                       FROM table_name
                      )
DELETE 
FROM table_name -- Or the CTE name depending on your SQL dialect
WHERE some_unique_id_column IN (SELECT some_unique_id_column
                                FROM DuplicateCTE
                                WHERE row_num > 1
                               )
;
"""

WITH DUPLICATE1 AS ( SELECT *, row_number() over( PARTITION BY CUSTOMERID ORDER BY customerid) as RN
                     FROM Customers
                    ) 
DELETE 
FROM  Customers
WHERE customerid IN ( SELECT customerid 
                      FROM DUPLICATE1
                      WHERE rn> 1
                     )
;

SELECT * FROM Customers;



----------------------------TOP 3 HIGHEST SALARIES------------------------------

--Q2 Get the top 3 highest salaries from Employee table
 with t1 as (
             SELECT SALARY, DENSE_RANK() OVER(ORDER BY SALARY DESC) AS R_SAL
             FROM   ACS_SBOX0.HBL_TEST_02_02_EMP
             WHERE SALARY IS NOT NULL
             GROUP BY SALARY
            )
 SELECT * 
 FROM T1 
 WHERE R_SAL <=3
 ;



-- QUESTION 15
""" ------------- FIND CUSTOMERS WITH ORDERS ABOVE AVG ORDER VALUE -------------------"""


WITH CTE AS (  SELECT  CUSTOMERID, ORDER_ID, SUM(QUANTITY * PRICE) AS ORDER_VALUE
               FROM   CUSTOMERS    AS C 
               GROUP BY CUSTOMERID, ORDER_ID
            ) 

SELECT CUSTOMERID, AVG(ORDER_VALUE), (SELECT AVG(ORDER_VALUE) FROM CTE) AS AVG_ORDER_VALUE
FROM CTE
WHERE ORDER_VALUE > (SELECT AVG(ORDER_VALUE) FROM CTE)
GROUP BY  CUSTOMERID
;


-- QUESTION 20
"""
FIND CUSTOMERS WHO PLACED ORDERS EVERY MONTH IN 2024 

"""

WITH CTE AS (
              SELECT   CUSTOMERID
                       ,EXTRACT(MONTH FROM ORDER_DATE) AS MONTH
              FROM   CUSTOMERS    AS C 
              WHERE EXTRACT(YEAR FROM ORDER_DATE) = 2024
            )
SELECT CUSTOMERID, MONTH
FROM CTE
GROUP BY CUSTOMERID, MONTH
HAVING SUM(DISTINCT MONTH)= 12
;



-- QUESTION 
""" ------------- Moving(running total) Average-------------------"""
 
 --The following query calculates a 7-day moving average of sales from a sales table 
 --with sale_date and sales_amount columns. 

WITH T1 AS (
            SELECT   ORDER_DATE, SUM(QUANTITY*PRICE) AS SALES
            FROM   CUSTOMERS    AS C 
            GROUP BY ORDER_DATE
           )

SELECT  ORDER_DATE, SALES
      ,SUM(SALES) OVER(ORDER BY ORDER_DATE ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CUMULATIVE
FROM T1

;


--Q 21: using the table PRODUCT_SELLS.... use a CTE and calculate the moving SUM of sales for the last 3 months
--Grouping by Category (e.g., Product)
--This calculates the moving average for different product categories independently, 
--you can use the PARTITION BY clause within the OVER clause. 

WITH T1 AS ( SELECT  
                    ORDER_DATE
                   ,CUSTOMERID
                   ,EXTRACT(month from ORDER_DATE) AS MTH
                   ,SUM(QUANTITY * PRICE)                   AS SALES 
            FROM CUSTOMERS
            WHERE CUSTOMERID=5 
            GROUP BY CUSTOMERID, ORDER_DATE, EXTRACT(month from ORDER_DATE) 
            )
SELECT    ORDER_DATE , MTH, CUSTOMERID,SALES 
         ,SUM(SALES)  OVER( PARTITION BY CUSTOMERID ORDER BY MTH  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) AS RUN

FROM T1



"""QUESTION 23 --sHOW PRODUCT SALE DISTRIBUTION(PERCENT OF TOTAL) FOR ALL PRODUCTS"""

--option 1: SUBQUERY IN TEH SELECT 
            SELECT PRODUCTID 
                  ,SUM(QUANTITY * PRICE) AS PRODUCT_REVENUE 
                  ,SUM(QUANTITY * PRICE) * 100.0 / (SELECT SUM(QUANTITY * PRICE) FROM CUSTOMERS) AS PERCENT
            FROM CUSTOMERS
            GROUP BY PRODUCTID 



--OPTION 2: CTE AND SUBQUERY in the select 
WITH T1 AS (
            SELECT PRODUCTID
                  ,SUM(QUANTITY * PRICE) AS PRODUCT_REV
                  ,(SELECT  SUM(QUANTITY * PRICE) 
                    FROM CUSTOMERS AS C
                    JOIN   PRODUCTS    AS P   ON C.PRODUCTID=P.PRODUCTID 
                   ) AS TOTAL_REVENUE
            FROM  CUSTOMERS AS C 
            GROUP BY PRODUCTID, TOTAL_REVENUE
           )
SELECT PRODUCTID, PRODUCT_REV, TOTAL_REVENUE, PRODUCT_REV * 100.0 / TOTAL_REVENUE AS PERCENT
FROM T1
;

-- OPTION 3 CTE and CROSS JOIN(). CROSS JOIN DOES NOT NEED AN 'ON' CONDITION
WITH T1 AS (
             SELECT  SUM(QUANTITY * PRICE) as TOTAL_REVENUE
             FROM CUSTOMERS AS C
           ) 
SELECT PRODUCTID 
            ,SUM(QUANTITY * PRICE ) AS PRODUCT_REV
            ,SUM(QUANTITY * PRICE ) * (100.0)  /  TOTAL_REVENUE   AS PERCENT
            ,TOTAL_REVENUE
FROM CUSTOMERS AS C
CROSS JOIN T1
GROUP BY PRODUCTID , TOTAL_REVENUE
;





"""--QUESTION 24: customers with consecutive purchases (2 DAYS) """

WITH Prev_pur as ( SELECT CUSTOMERID, ORDER_ID, ORDER_DATE
                         ,LAG(ORDER_DATE) OVER( ORDER BY ORDER_DATE ) AS Prev_purchase 
                   FROM CUSTOMERS
                   ORDER BY CUSTOMERID
                 )
             SELECT *
             FROM Prev_pur 
             WHERE (PREV_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
             --DATEDIFF(ORDER_DATE , Prev_purchase ) =1 
;

--Q5 Get a price of the previous day in each row
-- https://www.google.com/search?q=LAG+SQL&client=firefox-b-d&sca_esv=61c011a888dcc1e5&sxsrf=AE3TifPNbAuTPr616lgXVOT2z45wHaRh7w%3A1765253772341&ei=jKI3ad7CFPavqtsP64LSOQ&ved=2ahUKEwj28rbl0q-RAxWHkmoFHdMvMHUQ0NsOegUIsQEQAA&uact=5&sclient=gws-wiz-serp&udm=50&fbs=AIIjpHx4nJjfGojPVHhEACUHPiMQht6_BFq6vBIoFFRK7qchKEWEvuc0Hbw31oEI7c8o3y7EH2T73cHYgsE1-NZATxMpEZX5iGApNXoQtOTNvMiIjKZWgUwBEjxgbXqOA63Ym7bKTF3V6P7RsnGZeXWZ9Vg2Nnats9AsxvirX8wkyAN95VBBkPKee6raV0OTBiYd4C8msbFuvBQQju6Ki_6l2qtQDnLW7grWufszQSaiNGlC1iOCeik&aep=10&ntc=1&mstk=AUtExfCnL-Ijr0VQGioikxQhYGBMeCYIO-nN7C00osHBvxbSUJuEOJ5DtV5v2jAvyEDn_opjJlSjzg2CTwkAG0_jmeYAqoHoBqJCpInOw3MiX2MR8LVNZK_ZZOerhL9Wzs6YlLu7Khz7Jbb0ESySbirTa69HPChlR7A-30uQneZlMHVCRN_079_43Pwl6MiLr-kYfv71ScAgT5P1ysdkyn36gunvVsPuUYjqAzwzfH_ZkO8T0sLncjUQtfwjY750u1l0Gl5kXdFlTJG5omcOivDMVzO2s8AnUfYe6Z3Z07b1xCRUuIqwgzJlc3Hm7hUdGh9_efk5EmOM3RTp_g&csuir=1&mtid=m6Y3abnIGom3wN4PhrOHgA4
-- LAG (expresión [, offset [, valor_predeterminado]]) OVER ( [PARTITION BY cláusula_partición] ORDER BY cláusula_orden)
 --expresión: El nombre de la columna o el valor que se desea recuperar de la fila anterior.
 --offset: (Opcional) El número de filas hacia atrás desde la fila actual que se desea buscar. El valor predeterminado es 1.
 --valor_predeterminado: (Opcional) El valor que se devuelve si el offset queda fuera del alcance de la partición (por ejemplo, para la primera fila). Si no se especifica, se devuelve NULL.
 --OVER cláusula:
    --* PARTITION BY: (Opcional) Divide el conjunto de resultados en grupos separados (particiones), y la función LAG() se aplica dentro de cada partición de forma independiente.
    --* ORDER BY: (Obligatorio) Define el orden lógico de las filas dentro de cada partición, lo cual es crucial para determinar cuál es la fila "anterior" o "siguiente"

--Casos de Uso Comunes

  -- Análisis de tendencias: Comparar el rendimiento actual (ventas, precios de acciones, etc.) con períodos anteriores (día a día, mes a mes).
  -- Cálculo de diferencias: Determinar la variación entre filas consecutivas.
  -- Análisis de usuarios: Rastrear cambios en el estado o comportamiento del usuario a lo largo del tiempo. 

SELECT
    sale_date,
    sales_amount,
    LAG(sales_amount, 1) OVER (ORDER BY sale_date) AS previous_day_sales
FROM sales;

---------*********---------*********---------*********---------*********---------*********---------*********---------*********---------*********




""" question 25 Find churn customers (no orders in the last 6 months) """

         SELECT CUSTOMERID
                      ,MAX(ORDER_DATE) AS LAST_ORDER
                     ,NOW()
          GROUP BY CUSTOMERID

          FROM CUSTOMERS

          HAVING LAST_ORDER < ( NOW()   - INTERVAL 6 MONTH)




""" QUQESTION 28 GET CUSTOMERS WHO ORDERED MORE THAN THE AVG NUMBER OF ORDERS PER CUSTOMER """


WITH T1 AS (
                     SELECT C.CUSTOMERID, COUNT(C.ORDER_ID) AS ORDER_CNT
                     FROM ORDERS AS C
                     GROUP BY C.CUSTOMERID
                    ) 

                     SELECT CUSTOMERID, (SELECT AVG(ORDER_CNT) FROM T1 ) AS AVG_ORDER
FROM T1
WHERE ORDER_CNT > (SELECT  AVG(ORDER_CNT) FROM T1)
GROUP BY CUSTOMERID, ORDER_CNT
;





"""QUESTION 32 Find products that contribute to 80% of revenue(pareto analysis) """

1 I need to BREAK the code into 4 parts
P1: CTE1 to get revenue by product 
P2: CTE2 to get total revenue; base on the CTE1 data
P3: Select product, product_revenue and cumulative which come from a subquery
     The subquery uses 
     -A cross join of the CTE1 with CTE2 and 
     -A window function to calculate the cumulative revenue out of the product_revenue
P4: Filter the result to get products that contribute to +80% of the revenue; cumulative >= 80%


""" STRUCTURE OF THE SOLUTION:
     CTE1 AS ( SELECT PRODUCTID, SUM(QUANTITY * PRICE) AS P_REVENUE
               FROM dev.spark_db.customers as C
               GROUP BY PRODUCTID
             ),

     CTE2 AS ( SELECT SUM(P_REVENUE) AS TOTAL
               FROM CTE1
             )

SELECT        PRODUCTID, P_REVENUE, CUMULATIVE
FROM ( SELECT PRODUCTID, P_REVENUE, TOTAL
              ,SUM(P_REVENUE) OVER( ORDER BY P_REVENUE DESC) AS CUMULATIVE
       FROM CTE1
       CROSS JOIN CTE2
       GROUP BY PRODUCTID, P_REVENUE, TOTAL
      ) AS T
WHERE CUMULATIVE <= TOTAL * 0.80
"""



WITH PR_REV AS ( SELECT PRODUCTID, SUM(QUANTITY * PRICE) AS PRODUCT_REV
                 FROM CUSTOMERS AS C
                 GROUP BY PRODUCTID
               ),
                             
    TTL_REV AS ( SELECT SUM(PRODUCT_REV) AS TOTAL
                    FROM PR_REV 
               )

SELECT           PRODUCTID,   PRODUCT_REV, CUMULATIVE
FROM ( SELECT  P.PRODUCTID, P.PRODUCT_REV, TOTAL
              ,SUM(PRODUCT_REV) OVER(ORDER BY PRODUCT_REV DESC) AS CUMULATIVE
       FROM  PR_REV  AS P 
       CROSS JOIN TTL_REV AS TTR 
      ) AS FP
WHERE CUMULATIVE <= total * 0.85

;

--/////// OPTION TWO

WITH Revenue_CTE AS (
    SELECT product_id,
           SUM(sales_amount) AS total_revenue
    FROM sales
    GROUP BY product_id
),
Cumulative_Revenue AS (
    SELECT
        product_id,
        total_revenue,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
        SUM(total_revenue) OVER () AS overall_revenue
    FROM Revenue_CTE
)
SELECT
    product_id,
    total_revenue,
    cumulative_revenue,                                                                                                       
       overall_revenue,     
       (cumulative_revenue / overall_revenue) * 100 AS revenue_percentage
FROM Cumulative_Revenue
WHERE (cumulative_revenue / overall_revenue) <= 0.8
ORDER BY total_revenue DESC;       


-- option THREE DISPLAY EACH PRODUCT PERCENTAGE CONTRIBUTTION( whithout 80% rule FILTER).

WITH PRODREV AS ( SELECT PRODUCTID, SUM(QUANTITY * PRICE) AS PRODUCT_REVENUE
                                  FROM CUSTOMERS AS C 
                                  GROUP BY PRODUCTID
                                ),
          TOTALREV AS ( SELECT SUM(PRODUCT_REVENUE) AS TOTAL
                                  FROM PRODREV 
                                )
SELECT  PRODUCTID, PRODUCT_REVENUE, SUM(PRODUCT_REVENUE* 100.0 /TOTAL) PERCENT 
FROM( SELECT PRODUCTID, PRODUCT_REVENUE, TOTAL
                        ,SUM(PRODUCT_REVENUE) OVER(ORDER BY PRODUCT_REVENUE DESC) AS CUMULATIVE 
             FROM PRODREV AS PR
             CROSS JOIN TOTALREV AS TR
          ) AS T
GROUP BY  PRODUCTID, PRODUCT_REVENUE
;




""" QUESTION 34   CALCULATE AVG TIME BETWEEN TWO PURCHASES FOR EACH CUSTOMER    """


-- LAG Partition by CUSTOMERID so that we get the previous purchase date for the same customer.
-- COALESCE  fills nulls records with 0 so they are considered in the average count cause, basically LAG gets null when there is no previous purchase 
-- and nulls are not counted in the average  and so we need to fill them with 0 to get the average of all the purchases.

WITH PREV_PUR AS (
                  SELECT CUSTOMERID, ORDER_DATE
                         ,LAG(ORDER_DATE) OVER(PARTITION BY CUSTOMERID ORDER BY ORDER_DATE) AS PREVIOUS_PUR_DATE
                         ,COALESCE( DATEDIFF(ORDER_DATE, LAG(ORDER_DATE) OVER(PARTITION BY CUSTOMERID ORDER BY ORDER_DATE) ), 0) AS DIFF
                  FROM dev.spark_db.customers
                  order by customerid
                 ),
    SUM_DFF AS (
                  SELECT CUSTOMERID, ORDER_DATE, PREVIOUS_PUR_DATE, SUM(DIFF) AS SUM_DIFF
                  FROM PREV_PUR
                  GROUP BY CUSTOMERID, ORDER_DATE, PREVIOUS_PUR_DATE
                 )

SELECT CUSTOMERID, AVG(SUM_DIFF) AS AVG_DIFF
FROM SUM_DFF 
GROUP BY CUSTOMERID
ORDER BY CUSTOMERID DESC
;



"""Q 35 Find the YoY Growth Rate (%): """
--Formula: ((Current Period Revenue - Prior Period Revenue) / Prior Period Revenue) * 100
-- YOU CAN use the division or not.

WITH REV_YEAR AS (
             SELECT EXTRACT( YEAR FROM ORDER_DATE) AS YEAR, SUM(quantity * PRICE) AS REVENUE 
             FROM  CUSTOMERS
            GROUP BY  EXTRACT( YEAR FROM ORDER_DATE) 
           )      
 
SELECT YEAR, REVENUE ,  (REVENUE  - ( LAG(REVENUE ) OVER( ORDER BY YEAR)  ) ) * 100.0 AS YOY_GROWTH

FROM REV_YEAR 
GROUP BY YEAR, REVENUE 

;






"""--Q 37 RETRIEVE THE LONGEST GAP BETWEEN ORDERS FOR EACH CUSTOMER """

WITH PREVIOUS_ORDER_DATE AS (
                              SELECT CUSTOMERID, order_date
                                 ,LAG(ORDER_DATE) OVER(ORDER BY ORDER_DATE) AS PREV_DATE 
                              FROM CUSTOMERS
                            ),
     DATE_DIFFERENCE AS(
                        SELECT CUSTOMERID, ORDER_DATE, PREV_DATE, DATEDIFF(ORDER_DATE, PREV_DATE) AS DIFFERENT_DATE
                        FROM PREVIOUS_ORDER_DATE
                       )

SELECT CUSTOMERID
       ,MAX(DIFFERENT_DATE) MAX_DIF
FROM     DATE_DIFFERENCE       
GROUP BY CUSTOMERID







--Q9 Find products that contribute to 80% of revenue(pareto analysis)

WITH SALES_CTE AS ( SELECT PRODUCT_ID
                             , SUM(QUANITTY * PRICE) AS P_revenue
                        FROM   ORDERS
                        GROUP BY PRODUCT_ID
                   ), 

   total_revenue AS ( SELECT SUM(P_revenue) AS total 
                      FROM SALES_CTE
                     ),
SELECT PRODUCT_ID, P_revenue, Cumulative_Revenue
FROM (SELECT S.PRODUCT_ID, S.P_revenue
             , SUM(S.P_revenue) OVER (ORDER BY S.P_revenue DESC) AS Cumulative_Revenue
             , T.total
             FROM SALES_CTE S
             CROSS JOIN total_revenue T
     )
WHERE Cumulative_Revenue <= total * 0.8;



--Q10 




--DELETE
--YOU CAN CREATE AND INSERT SIMULTANEOUSLYOR
SELECT * FROM ACS_SBOX0.HBL_TEST_03_03_CTRY

DROP TABLE ACS_SBOX0.HBL_TEST_03_03_CTRY IF EXISTS;


CREATE TABLE ACS_SBOX0.HBL_TEST_03_03_CTRY ( 
                                             ID    varchar(400)  NOT NULL 
                                            ,NAME  varchar(400)  NOT NULL
                                            ,AGE   varchar(400)  NOT NULL                                            

                                           );
                                     
INSERT INTO   ACS_SBOX0.HBL_TEST_03_03_CTRY (ID, NAME ,AGE)
       VALUES  ('1', 'A', '21')
             , ('2', 'B', '23')
             , ('2', 'B', '23')
             , ('4', 'D', '22')
             , ('5', 'E', '25')
             , ('5', 'E', '25')
             , ('6', 'G', '26')

              
              
WITH ur

--DELETE





--YOU CAN CREATE AND INSERT SIMULTANEOUSLYOR
DROP TABLE ACS_SBOX0.HBL_TEST_01_06_CTRY IF EXISTS;


CREATE TABLE ACS_SBOX0.HBL_TEST_01_06_CTRY ( 
                                             SSN varchar(400) NOT NULL 
                                            ,COUNTRY_CODE   varchar(400)    NOT NULL
                                            ,COUNTRY_NAME   varchar(400)    NOT NULL                                            
                                            ,CONTINENT_CODE varchar(400)    NOT NULL
                                            ,INVEST         FLOAT           NOT NULL

                                           );
                                     
INSERT INTO   ACS_SBOX0.HBL_TEST_01_06_CTRY (SSN, COUNTRY_CODE ,COUNTRY_NAME, CONTINENT_CODE, INVEST)
       VALUES  ('0001234', '01', 'INDIA','AS', 1500 )                   ,  ('0001235' , '02', 'SOUTH AFRICA' ,'SF', 1500)
              ,('0001236', '03', 'UNITED STATES of AMERICA','NA', 2000) ,  ('0001237', '04', 'BRAZIL','SA', 2500)
              ,('0001236', '03', 'UNITED STATES of AMERICA','NA', 2000) ,  ('0001241', '08', 'MEXICO','MX', 4000)              
              ,('0001238', '05', 'PERU','PE', 2000)                     ,  ('0001239', '06', 'AUSTRALIA' ,'AU', 3000)              
WITH ur






