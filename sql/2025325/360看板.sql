create database dev_realtime_v1;


use dev_realtime_v1;

drop table ods_product_base;
CREATE external  TABLE if not exists ods_product_base (
  product_id int ,
  product_name string ,
  product_category string ,
  product_type int ,
  price decimal(10,2) ,
  cost decimal(10,2) ,
  create_time string ,
  update_time string ,
  is_active int ,
  tags string
)COMMENT '商品基础信息表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_base"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_base' into table ods_product_base partition (ds = '20250330')

select * from ods_product_base;

drop table ods_product_guide_ability;
CREATE external TABLE if not exists ods_product_guide_ability (
  product_id int ,
  product_name string,
  product_type int,
  date_str string ,
  guide_uv int ,
  guide_pv int ,
  guide_session_count int ,
  guide_cart_count int ,
  guide_favorite_count int ,
  guide_order_count int ,
  guide_payment_count int ,
  guide_payment_amount decimal(12,2) ,
  related_product_count int ,
  avg_related_products decimal(10,2) ,
  avg_guide_duration decimal(10,2) ,
  guide_uv_rank int ,
  guide_order_rank int ,
  create_time string ,
  update_time string ,
  ext_info string
)COMMENT '商品引导能力表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_guide_ability"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_guide_ability' into table ods_product_guide_ability partition (ds= '20250330')

select * from ods_product_guide_ability;


drop table ods_product_guide_conversion;
CREATE external TABLE if not exists ods_product_guide_conversion (
  guide_id int ,
  source_product_id int ,
  target_product_id int ,
  user_id int ,
  visit_time string ,
  conversion_type int ,
  conversion_time string ,
  is_order int ,
  order_id string ,
  order_amount decimal(10,2)
)COMMENT ' 商品引导转化表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_guide_conversion"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_guide_conversion' into table ods_product_guide_conversion partition (ds='20250330')

select * from ods_product_guide_conversion;

drop table ods_product_relation;
CREATE external TABLE if not exists ods_product_relation (
  main_product_id int ,
  related_product_id int ,
  relation_type int ,
  relation_count int ,
  relation_users int ,
  last_relation_time string
)COMMENT ' 商品关联关系表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_relation"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_relation' into table ods_product_relation partition (ds=20250330)

select * from ods_product_relation;


drop table ods_product_relation_behavior;
CREATE external TABLE if not exists ods_product_relation_behavior (
  behavior_id int ,
  user_id int ,
  main_product_id int ,
  related_product_id int ,
  behavior_type int ,
  behavior_time string ,
  session_id string ,
  device_type int
)COMMENT ' 商品关联行为表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_relation_behavior"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);
load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_relation_behavior' into table ods_product_relation_behavior partition (ds='20250330')

select * from ods_product_relation_behavior;


drop table ods_product_sales_summary;
CREATE external TABLE if not exists ods_product_sales_summary (
  product_id int ,
  date_str string ,
  visit_count int ,
  uv int ,
  cart_count int ,
  favorite_count int ,
  order_count int ,
  payment_count int ,
  payment_amount decimal(12,2) ,
  guide_visit_count int ,
  guide_order_count int ,
  guide_payment_amount decimal(12,2)
)COMMENT ' 商品销售汇总表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_sales_summary"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_sales_summary' into table ods_product_sales_summary partition (ds='20250330')

select * from ods_product_sales_summary;


drop table ods_product_visit_behavior;
CREATE external TABLE if not exists ods_product_visit_behavior (
  visit_id int ,
  user_id int ,
  product_id int ,
  visit_time string ,
  stay_duration int ,
  page_url string ,
  referer_url string ,
  device_type int ,
  ip_address string ,
  province string ,
  city string
)COMMENT ' 商品访问行为表'
PARTITIONED BY (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/warehouse/dev_realtime_v1/ods/ods_product_visit_behavior"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
);

load data inpath '/dev_realtime_v1_jingnan_guo/2025-03-30/product_visit_behavior' into table ods_product_visit_behavior partition (ds='20250330')

select * from ods_product_visit_behavior;



-- DWS 层

use dev_realtime_v1;

    -- 创建 DWS 层表
    CREATE external TABLE IF NOT EXISTS dws_product_metrics_enhanced (
        product_id INT,
        product_name STRING,
        product_category STRING,
        product_type INT,
        -- 近 7 天数据
        recent_7d_guide_uv INT,
        recent_7d_guide_order_count INT,
        recent_7d_guide_payment_amount DECIMAL(12, 2),
        recent_7d_visit_count INT,
        recent_7d_order_count INT,
        recent_7d_payment_amount DECIMAL(12, 2),
        recent_7d_related_product_count INT,
        -- 实时监控数据
        realtime_guide_uv INT,
        realtime_guide_order_count INT,
        realtime_guide_payment_amount DECIMAL(12, 2),
        realtime_visit_count INT,
        realtime_order_count INT,
        realtime_payment_amount DECIMAL(12, 2),
        realtime_related_product_count INT,
        -- 关联关系指标
        concurrent_visit_relation_count INT,
        concurrent_favorite_cart_relation_count INT,
        concurrent_payment_relation_count INT,
        -- 连带商品明细
        related_product_details STRING,
        -- 时间维度标记
        time_dimension STRING
    )COMMENT ' 商品访问行为表'
    PARTITIONED BY (ds string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
    LOCATION "/warehouse/dev_realtime_v1/dws/dws_product_metrics_enhanced"
    tblproperties (
            'hive.exec.compress.output' = 'true',
            'mapreduce.output.fileoutputformat.compress' = 'true',
            'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );

    -- 插入近 7 天数据
INSERT INTO dws_product_metrics_enhanced partition (ds='20250330')
SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    p.product_type,
    -- 近 7 天商品引导能力指标
    COALESCE(ga7.recent_7d_guide_uv, 0) AS recent_7d_guide_uv,
    COALESCE(ga7.recent_7d_guide_order_count, 0) AS recent_7d_guide_order_count,
    COALESCE(ga7.recent_7d_guide_payment_amount, 0) AS recent_7d_guide_payment_amount,
    -- 近 7 天销售指标
    COALESCE(ss7.recent_7d_visit_count, 0) AS recent_7d_visit_count,
    COALESCE(ss7.recent_7d_order_count, 0) AS recent_7d_order_count,
    COALESCE(ss7.recent_7d_payment_amount, 0) AS recent_7d_payment_amount,
    -- 近 7 天关联商品数量
    COALESCE(r7.recent_7d_related_product_count, 0) AS recent_7d_related_product_count,
    -- 实时监控数据设为 0（后续可根据实时需求修改）
    0 AS realtime_guide_uv,
    0 AS realtime_guide_order_count,
    0 AS realtime_guide_payment_amount,
    0 AS realtime_visit_count,
    0 AS realtime_order_count,
    0 AS realtime_payment_amount,
    0 AS realtime_related_product_count,
    -- 关联关系指标
    COALESCE(rb7.concurrent_visit_relation_count, 0) AS concurrent_visit_relation_count,
    COALESCE(rb7.concurrent_favorite_cart_relation_count, 0) AS concurrent_favorite_cart_relation_count,
    COALESCE(rb7.concurrent_payment_relation_count, 0) AS concurrent_payment_relation_count,
    -- 连带商品明细
    r.related_product_details,
    'recent_7d' AS time_dimension
FROM
    ods_product_base p
        -- 关联近 7 天商品引导能力指标
        LEFT JOIN (
        SELECT
            product_id,
            SUM(guide_uv) AS recent_7d_guide_uv,
            SUM(guide_order_count) AS recent_7d_guide_order_count,
            SUM(guide_payment_amount) AS recent_7d_guide_payment_amount
        FROM
            ods_product_guide_ability
        WHERE
                date_str >= date_sub(current_date, 7)
        GROUP BY
            product_id
    ) ga7 ON p.product_id = ga7.product_id
        -- 关联近 7 天销售汇总指标
        LEFT JOIN (
        SELECT
            product_id,
            SUM(visit_count) AS recent_7d_visit_count,
            SUM(order_count) AS recent_7d_order_count,
            SUM(payment_amount) AS recent_7d_payment_amount
        FROM
            ods_product_sales_summary
        WHERE
                date_str >= date_sub(current_date, 7)
        GROUP BY
            product_id
    ) ss7 ON p.product_id = ss7.product_id
        -- 关联近 7 天关联商品数量
        LEFT JOIN (
        SELECT
            main_product_id,
            COUNT(DISTINCT related_product_id) AS recent_7d_related_product_count
        FROM
            ods_product_relation
        WHERE
                last_relation_time >= date_sub(current_date, 7)
        GROUP BY
            main_product_id
    ) r7 ON p.product_id = r7.main_product_id
        -- 关联近 7 天关联关系指标
        LEFT JOIN (
        SELECT
            main_product_id,
            SUM(CASE WHEN behavior_type = 1 THEN 1 ELSE 0 END) AS concurrent_visit_relation_count,
            SUM(CASE WHEN behavior_type = 2 THEN 1 ELSE 0 END) AS concurrent_favorite_cart_relation_count,
            SUM(CASE WHEN behavior_type = 3 THEN 1 ELSE 0 END) AS concurrent_payment_relation_count
        FROM
            ods_product_relation_behavior
        WHERE
                behavior_time >= date_sub(current_date, 7)
        GROUP BY
            main_product_id
    ) rb7 ON p.product_id = rb7.main_product_id
        -- 关联连带商品明细
        LEFT JOIN (
        SELECT
            main_product_id,
            concat_ws(',', collect_list(cast(related_product_id AS STRING))) AS related_product_details
        FROM
            ods_product_relation
        GROUP BY
            main_product_id
    ) r ON p.product_id = r.main_product_id;