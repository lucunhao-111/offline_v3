DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`             STRING COMMENT '下单时间',
    `payment_date_id`        STRING COMMENT '支付日期ID',
    `payment_time`           STRING COMMENT '支付时间',
    `finish_date_id`         STRING COMMENT '确认收货日期ID',
    `finish_time`             STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`         DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
TBLPROPERTIES ('orc.compress' = 'snappy');


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(dt)
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'99991231')
from
(
    select
        id,
        user_id,
        province_id,
        create_time,
        original_total_amount,
        activity_reduce_amount,
        coupon_reduce_amount,
        total_amount
    from ods_order_info
    where dt=${d}
)oi
left join
(
    select
        order_id,
        callback_time,
        total_amount payment_amount
    from ods_payment_info
    where dt=${d}
    and payment_status='1602'
)pi
on oi.id=pi.order_id
left join
(
    select
       order_id,
       create_time finish_time
    from ods_order_status_log
    where dt=${d}
    and order_status='1004'
)log
on oi.id=log.order_id;



set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(dt)
select
    oi.order_id,
    user_id,
    province_id,
    order_date_id,
    order_time,
    nvl(oi.payment_date_id,pi.payment_date_id),
    nvl(oi.payment_time,pi.payment_time),
    nvl(oi.finish_date_id,log.finish_date_id),
    nvl(oi.finish_time,log.finish_time),
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    nvl(oi.payment_amount,pi.payment_amount),
    nvl(nvl(oi.finish_time,log.finish_time),'99991231')
from
(
    select
        order_id,
        user_id,
        province_id,
        order_date_id,
        order_time,
        payment_date_id,
        payment_time,
        finish_date_id,
        finish_time,
        order_original_amount,
        order_activity_amount,
        order_coupon_amount,
        order_total_amount,
        payment_amount
    from dwd_trade_trade_flow_acc
    where dt='99991231'
    union all
    select
        id,
        user_id,
        province_id,
        date_format(create_time,'yyyy-MM-dd') order_date_id,
        create_time,
        null payment_date_id,
        null payment_time,
        null finish_date_id,
        null finish_time,
        original_total_amount,
        activity_reduce_amount,
        coupon_reduce_amount,
        total_amount,
        null payment_amount
    from ods_order_info
    where dt='20220609'
)oi
left join
(
    select
        order_id,
        date_format(callback_time,'yyyy-MM-dd') payment_date_id,
        callback_time payment_time,
        total_amount payment_amount
    from ods_payment_info
    where dt='2022-06-09'
    and array_contains(map_keys(old),'payment_status')
    and payment_status='1602'
)pi
on oi.order_id=pi.order_id
left join
(
    select
        order_id,
        date_format(create_time,'yyyy-MM-dd') finish_date_id,
        create_time finish_time
    from ods_order_status_log
    where dt='2022-06-09'
    and order_status='1004'
)log
on oi.order_id=log.order_id;
