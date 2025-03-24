DROP TABLE IF EXISTS ods_order_detail;
CREATE EXTERNAL TABLE ods_order_detail(
                                          `id` STRING COMMENT '编号',
                                          `order_id` STRING COMMENT '订单号',
                                          `sku_id` STRING COMMENT '商品 id',
                                          `sku_name` STRING COMMENT '商品名称',
                                          `order_price` DECIMAL(16,2) COMMENT '商品价格',
                                          `sku_num` BIGINT COMMENT '商品数量',
                                          `create_time` STRING COMMENT '创建时间',
                                          `source_type` STRING COMMENT '来源类型',
                                          `source_id` STRING COMMENT '来源编号',
                                          `split_final_amount` DECIMAL(16,2) COMMENT '分摊最终金额',
                                          `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动优惠',
                                          `split_coupon_amount` DECIMAL(16,2) COMMENT '分摊优惠券优惠'
) COMMENT '订单详情表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods_order_detail'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/order_detail' into table ods_order_detail partition (dt='2025-03-24');;
select * from ods_order_detail;


DROP TABLE IF EXISTS ods_order_info;
CREATE EXTERNAL TABLE ods_order_info (
                                         `id` STRING COMMENT '订单号',
                                         `final_amount` DECIMAL(16,2) COMMENT '订单最终金额',
                                         `order_status` STRING COMMENT '订单状态',
                                         `user_id` STRING COMMENT '用户 id',
                                         `payment_way` STRING COMMENT '支付方式',
                                         `delivery_address` STRING COMMENT '送货地址',
                                         `out_trade_no` STRING COMMENT '支付流水号',
                                         `create_time` STRING COMMENT '创建时间',
                                         `operate_time` STRING COMMENT '操作时间',
                                         `expire_time` STRING COMMENT '过期时间',
                                         `tracking_no` STRING COMMENT '物流单编号',
                                         `province_id` STRING COMMENT '省份 ID',
                                         `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免金额',
                                         `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免金额',
                                         `original_amount` DECIMAL(16,2) COMMENT '订单原价金额',
                                         `feight_fee` DECIMAL(16,2) COMMENT '运费',
                                         `feight_fee_reduce` DECIMAL(16,2) COMMENT '运费减免'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods_order_info'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/order_info' into table ods_order_info partition (dt='2025-03-24');;
select * from ods_order_info;

DROP TABLE IF EXISTS ods_coupon_use;
CREATE EXTERNAL TABLE ods_coupon_use(
                                        `id` STRING COMMENT '编号',
                                        `coupon_id` STRING COMMENT '优惠券 ID',
                                        `user_id` STRING COMMENT 'skuid',
                                        `order_id` STRING COMMENT 'spuid',
                                        `coupon_status` STRING COMMENT '优惠券状态',
                                        `get_time` STRING COMMENT '领取时间',
                                        `using_time` STRING COMMENT '使用时间(下单)',
                                        `used_time` STRING COMMENT '使用时间(支付)',
                                        `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ods_coupon_use/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/origin_data/gmall/coupon_use' into table ods_coupon_use partition (dt='2025-03-24');;
select * from ods_coupon_use;