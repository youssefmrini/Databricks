-- Databricks notebook source
-- DBTITLE 1,Switching to Demo Delta Catalog and Example Schema
-- Use the specified catalog
use catalog demo_delta;

-- Use the specified schema
use schema example;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - parse_json: Constructs a variant from a json string
-- MAGIC - to_json:Converts a variant to a json string
-- MAGIC - variant_get: Extracts the path of specified type, from the variant
-- MAGIC - cast: cast to and from variant
-- MAGIC - schema_of_variant: Returns the schema string of variant
-- MAGIC - variant_explode: Table function for un-nesting a variant

-- COMMAND ----------

-- DBTITLE 1,Parse_json
CREATE or replace TABLE store_data AS
SELECT parse_json(
  '{
    "store":{
        "fruit": [
          {"weight":8,"type":"apple"},
          {"weight":9,"type":"pear"}
        ],
        "basket":[
          [1,2,{"b":"y","a":"x"}],
          [3,4],
          [5,6]
        ],
        "book":[
          {
            "author":"Nigel Rees",
            "title":"Sayings of the Century",
            "category":"reference",
            "price":8.95
          },
          {
            "author":"Herman Melville",
            "title":"Moby Dick",
            "category":"fiction",
            "price":8.99,
            "isbn":"0-553-21311-3"
          },
          {
            "author":"J. R. R. Tolkien",
            "title":"The Lord of the Rings",
            "category":"fiction",
            "reader":[
              {"age":25,"name":"bob"},
              {"age":26,"name":"jack"}
            ],
            "price":22.99,
            "isbn":"0-395-19395-8"
          }
        ],
        "bicycle":{
          "price":19.95,
          "color":"red"
        }
      },
      "owner":"amy",
      "zip code":"94025",
      "fb:testid":"1234"
  }'
) as raw


-- COMMAND ----------

-- DBTITLE 1,Access the data
SELECT raw:store:bicycle['color'] FROM store_data


-- COMMAND ----------

SELECT key, value
  FROM store_data,
  LATERAL variant_explode(store_data.raw:store);

-- COMMAND ----------

-- DBTITLE 1,Create the table with Variant Type
CREATE OR REPLACE TABLE customer (data  VARIANT);


-- COMMAND ----------

-- DBTITLE 1,Copy the Data in the table
COPY INTO delta_demo.example.customer 
FROM '/Volumes/demo_delta/demo/you/file.json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('singleVariantColumn' = 'data')

-- COMMAND ----------

-- DBTITLE 1,Query the table
select * from demo_delta.example.customer

-- COMMAND ----------

-- DBTITLE 1,Get the Schema
select schema_of_variant(data) from demo_delta.example.customer


-- COMMAND ----------

-- DBTITLE 1,Access the data
SELECT 
    data:age AS age,
    data:city AS city,
    data:friends[0]['hobbies'] AS friend_0_hobbies,
    data:friends[0]['name'] AS friend_0_name,
    data:friends[1]['hobbies'] AS friend_1_hobbies,
    data:friends[1]['name'] AS friend_1_name,
    data:friends[2]['hobbies'] AS friend_2_hobbies,
    data:friends[2]['name'] AS friend_2_name,
    data:friends[3]['hobbies'] AS friend_3_hobbies,
    data:friends[3]['name'] AS friend_3_name,
    data:friends[4]['hobbies'] AS friend_4_hobbies,
    data:friends[4]['name'] AS friend_4_name,
    data:friends[5]['hobbies'] AS friend_5_hobbies,
    data:friends[5]['name'] AS friend_5_name
FROM 
    demo_delta.example.customer

-- COMMAND ----------

-- DBTITLE 1,Variant_get
 SELECT variant_get(data, '$.friends[0]["hobbies"]', 'ARRAY<STRING>') AS hobbies
    FROM demo_delta.example.customer

-- COMMAND ----------

-- DBTITLE 1,Variant Explode
WITH example AS (
    SELECT variant_get(data, '$.friends[0]["hobbies"]', 'STRING') AS hobbies
    FROM demo_delta.example.customer
)
SELECT  value 
FROM example, LATERAL variant_explode(parse_json(example.hobbies));
