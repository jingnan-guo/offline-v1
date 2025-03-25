
use gmall;

drop table dws_order_detail;
create table if not exists dws_order_detail(
                                               user_id int,
                                               order_num int,
                                               order_price decimal(16,2)
    )PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/dws/dws_order_detail'TBLPROPERTIES (
                                                                      'hive.exec.compress.output' = 'true',
                                                                      'mapreduce.output.fileoutputformat.compress' = 'true',
                                                                      'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
                                                                  );

insert into dws_order_detail partition (dt='2025-03-24')
select
    dod.user_id,
    dod.sku_num*dod.order_price as order_price,
    count(doi.id) as order_num
from dwd_order_detail dod,dwd_order_info doi,dwd_sku_info dsi
where dod.sku_id=dsi.id and doi.id=dod.order_id;


select * from dws_order_detail;