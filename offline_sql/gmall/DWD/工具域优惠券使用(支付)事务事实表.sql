DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_tool_coupon_used_inc partition(dt)
select
    id,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time,
    date_format(used_time,'yyyy-MM-dd')
from ods_coupon_use
where dt=${d}
and used_time is not null;