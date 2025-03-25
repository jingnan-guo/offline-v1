use gmall;

CREATE TABLE ads_gmv_summary (
                                 sales_amount DOUBLE,          -- 销售额
                                 cancelled_amount DOUBLE,      -- 取消订单金额
                                 rejected_amount DOUBLE,       -- 拒收订单金额
                                 returned_amount DOUBLE,       -- 退货订单金额
                                 GMV DOUBLE                    -- GMV 总额
)partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ads/ads_gmv_summary"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );

CREATE TABLE ads_conversion_rate (
                                     paid_orders INT,              -- 已支付订单数
                                     total_orders INT,             -- 总订单数
                                     conversion_rate DOUBLE        -- 转化率
)partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ads/ads_conversion_rate"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );

CREATE TABLE ads_repurchase_rate (
                                     tm_id STRING,                 -- 品牌 ID
                                     repurchase_users INT,         -- 复购用户数
                                     total_users INT,              -- 总购买用户数
                                     repurchase_rate DOUBLE        -- 复购率
)partitioned by (dt string)
row format delimited fields terminated by "\t"
location "/warehouse/gmall/ads/ads_repurchase_rate"
tblproperties (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );

//GMV成交总额
SELECT
    SUM(CASE WHEN order_status = '已支付' THEN total_amount ELSE 0 END) AS sales_amount, -- 销售额
    SUM(CASE WHEN order_status = '已取消' THEN total_amount ELSE 0 END) AS cancelled_amount, -- 取消订单金额
    SUM(CASE WHEN order_status = '拒收' THEN total_amount ELSE 0 END) AS rejected_amount, -- 拒收订单金额
    SUM(CASE WHEN order_status = '退货' THEN total_amount ELSE 0 END) AS returned_amount, -- 退货订单金额
    SUM(CASE WHEN order_status IN ('已支付', '已取消', '拒收', '退货') THEN total_amount ELSE 0 END) AS GMV -- GMV总额
FROM ods_order_info
WHERE dt = '2025-03-24';




INSERT INTO TABLE ads_gmv_summary PARTITION (dt='2025-03-24')
SELECT
    SUM(CASE WHEN order_status = '已支付' THEN total_amount ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN order_status = '已取消' THEN total_amount ELSE 0 END) AS cancelled_amount,
    SUM(CASE WHEN order_status = '拒收' THEN total_amount ELSE 0 END) AS rejected_amount,
    SUM(CASE WHEN order_status = '退货' THEN total_amount ELSE 0 END) AS returned_amount,
    SUM(CASE WHEN order_status IN ('已支付', '已取消', '拒收', '退货') THEN total_amount ELSE 0 END) AS GMV
FROM ods_order_info
WHERE dt = '2025-03-24';

INSERT INTO TABLE ads_conversion_rate PARTITION (dt='2025-03-24')
SELECT
    COUNT(DISTINCT CASE WHEN order_status = '已支付' THEN id END) AS paid_orders,
    COUNT(DISTINCT id) AS total_orders,
    (COUNT(DISTINCT CASE WHEN order_status = '已支付' THEN id END) * 1.0 / COUNT(DISTINCT id)) AS conversion_rate
FROM ods_order_info
WHERE dt = '2025-03-24';

INSERT INTO TABLE ads_repurchase_rate PARTITION (dt='2025-03-24')
WITH user_brand_first_purchase AS (
    SELECT
    user_id,
    tm_id, -- 品牌id
    MIN(order_id) AS first_purchase_order -- 第一次购买订单
    FROM ods_order_detail
    JOIN ods_sku_info ON ods_order_detail.sku_id = ods_sku_info.id
    WHERE dt = '2025-03-24'
    GROUP BY user_id, tm_id
    ),
    user_brand_repurchase AS (
    SELECT
    ods_order_detail.user_id,
    ods_sku_info.tm_id,
    COUNT(DISTINCT ods_order_detail.order_id) AS repurchase_count -- 复购次数
    FROM ods_order_detail
    JOIN ods_sku_info ON ods_order_detail.sku_id = ods_sku_info.id
    WHERE dt = '2025-03-24'
    GROUP BY ods_order_detail.user_id, ods_sku_info.tm_id
    )
-- 计算复购率
SELECT
    u.tm_id, -- 品牌id
    COUNT(DISTINCT CASE WHEN r.repurchase_count > 1 THEN u.user_id END) AS repurchase_users, -- 复购用户数
    COUNT(DISTINCT u.user_id) AS total_users, -- 总购买用户数
    (COUNT(DISTINCT CASE WHEN r.repurchase_count > 1 THEN u.user_id END) * 1.0 / COUNT(DISTINCT u.user_id)) AS repurchase_rate -- 复购率
FROM user_brand_first_purchase u
         LEFT JOIN user_brand_repurchase r ON u.user_id = r.user_id AND u.tm_id = r.tm_id
GROUP BY u.tm_id;