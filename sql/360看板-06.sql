
create database dev_realtime_v2;

use dev_realtime_v2;

drop table ods_tmall_new_product_conversion;
create external table if not exists ods_tmall_new_product_conversion(
  conversion_id int,
  product_id int,
  conversion_type string,
  conversion_time string ,
  conversion_step int,
  referral_product_id int
)COMMENT ' 商品转化信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v2/ods/ods_tmall_new_product_conversion"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v2/2025-04-01/tmall_new_product_conversion' into table ods_tmall_new_product_conversion partition (ds='20250401')

select * from ods_tmall_new_product_conversion;

drop table ods_tmall_new_product_info;
CREATE external TABLE if not exists ods_tmall_new_product_info (
  product_id int,
  product_title string,
  product_sku string,
  product_image_url string,
  is_new_product int,
  new_product_type string,
  category_id int,
  category_name string,
  create_time string ,
  brand_name string,
  product_price decimal(10,2) ,
  product_cost decimal(10,2) ,
  product_weight decimal(5,2) ,
  product_size string ,
  product_color string ,
  product_material string ,
  supplier_id int ,
  supplier_name string
) COMMENT ' 商品基础信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v2/ods/ods_tmall_new_product_info"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v2/2025-04-01/tmall_new_product_info' into table ods_tmall_new_product_info partition (ds='20250401')

select * from ods_tmall_new_product_info;

drop table ods_tmall_new_product_reviews;
CREATE external TABLE if not exists ods_tmall_new_product_reviews (
  review_id int ,
  product_id int ,
  reviewer_id int ,
  review_time string ,
  review_score int ,
  review_content string ,
  is_anonymous tinyint
)COMMENT ' 商品评价信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v2/ods/ods_tmall_new_product_reviews"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v2/2025-04-01/tmall_new_product_reviews' into table ods_tmall_new_product_reviews partition (ds='20250401')

select * from ods_tmall_new_product_reviews;


drop table ods_tmall_new_product_sales;
CREATE TABLE ods_tmall_new_product_sales (
                                             sales_id int ,
                                             product_id int,
                                             pay_amount decimal(10,2),
                                             pay_time string,
                                             order_id int,
                                             quantity int,
                                             discount_amount decimal(10,2),
                                             payment_method string,
                                             shipping_fee decimal(5,2),
                                             refund_amount decimal(10,2),
                                             refund_time string,
                                             order_status string
) COMMENT ' 商品销售数据表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v2/ods/ods_tmall_new_product_sales"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v2/2025-04-01/tmall_new_product_sales' into table ods_tmall_new_product_sales partition (ds='20250401')

select * from ods_tmall_new_product_sales;


drop table ods_tmall_new_product_visitors;
CREATE TABLE ods_tmall_new_product_visitors (
                                                visit_id int  ,
                                                product_id int ,
                                                visitor_id int ,
                                                visit_time string ,
                                                visitor_source string ,
                                                visitor_device string ,
                                                visitor_os string ,
                                                visitor_browser string ,
                                                visitor_region string
) COMMENT ' 商品访客信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v2/ods/ods_tmall_new_product_visitors"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v2/2025-04-01/tmall_new_product_visitors' into table ods_tmall_new_product_visitors partition (ds='20250401')

select * from ods_tmall_new_product_visitors;


---dws


use dev_realtime_v2;

drop table dws_tmall_new_products;
-- 创建 DWS 层表
CREATE external TABLE IF NOT EXISTS dws_tmall_new_products (
    -- 商品基础信息指标
    product_id INT,
    product_title STRING,
    product_sku STRING,
    product_image_url STRING,
    is_new_product INT,
    new_product_type STRING,
    category_id INT,
    category_name STRING,
    brand_name STRING,
    product_price DECIMAL(10, 2),
    product_cost DECIMAL(10, 2),
    product_weight DECIMAL(5, 2),
    product_size STRING,
    product_color STRING,
    product_material STRING,
    supplier_id INT,
    supplier_name STRING,
    -- 商品销售指标
    total_pay_amount DECIMAL(12, 2),
    total_quantity INT,
    total_discount_amount DECIMAL(10, 2),
    total_shipping_fee DECIMAL(5, 2),
    total_refund_amount DECIMAL(10, 2),
    order_count INT,
    -- 新品整体运营指标
    total_visitor_count INT,
    total_conversion_count INT,
    total_review_count INT,
    avg_review_score DECIMAL(3, 2)
)  COMMENT ' 商品访客信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v2/dws/dws_tmall_new_products"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

-- 从 ODS 层表联查数据并插入到 DWS 层表
INSERT INTO dws_tmall_new_products partition (ds='20250401')
SELECT
    -- 商品基础信息指标
    info.product_id,
    info.product_title,
    info.product_sku,
    info.product_image_url,
    info.is_new_product,
    info.new_product_type,
    info.category_id,
    info.category_name,
    info.brand_name,
    info.product_price,
    info.product_cost,
    info.product_weight,
    info.product_size,
    info.product_color,
    info.product_material,
    info.supplier_id,
    info.supplier_name,
    -- 商品销售指标
    SUM(COALESCE(sales.pay_amount, 0)) AS total_pay_amount,
    SUM(COALESCE(sales.quantity, 0)) AS total_quantity,
    SUM(COALESCE(sales.discount_amount, 0)) AS total_discount_amount,
    SUM(COALESCE(sales.shipping_fee, 0)) AS total_shipping_fee,
    SUM(COALESCE(sales.refund_amount, 0)) AS total_refund_amount,
    COUNT(DISTINCT sales.order_id) AS order_count,
    -- 新品整体运营指标
    COUNT(DISTINCT visitors.visitor_id) AS total_visitor_count,
    COUNT(DISTINCT conversion.conversion_id) AS total_conversion_count,
    COUNT(DISTINCT reviews.review_id) AS total_review_count,
    AVG(COALESCE(reviews.review_score, 0)) AS avg_review_score
FROM
    ods_tmall_new_product_info info
-- 关联商品销售数据表
        LEFT JOIN ods_tmall_new_product_sales sales ON info.product_id = sales.product_id AND info.ds = sales.ds
-- 关联商品访客信息表
        LEFT JOIN ods_tmall_new_product_visitors visitors ON info.product_id = visitors.product_id AND info.ds = visitors.ds
-- 关联商品转化信息表
        LEFT JOIN ods_tmall_new_product_conversion conversion ON info.product_id = conversion.product_id AND info.ds = conversion.ds
-- 关联商品评价信息表
        LEFT JOIN ods_tmall_new_product_reviews reviews ON info.product_id = reviews.product_id AND info.ds = reviews.ds
GROUP BY
    info.product_id,
    info.product_title,
    info.product_sku,
    info.product_image_url,
    info.is_new_product,
    info.new_product_type,
    info.category_id,
    info.category_name,
    info.brand_name,
    info.product_price,
    info.product_cost,
    info.product_weight,
    info.product_size,
    info.product_color,
    info.product_material,
    info.supplier_id,
    info.supplier_name,
    info.ds;

select * from dws_tmall_new_products;