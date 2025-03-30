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
)COMMENT '商品访问行为表'
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

