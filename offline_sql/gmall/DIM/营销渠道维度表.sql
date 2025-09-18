DROP TABLE IF EXISTS dim_promotion_refer_full;
CREATE EXTERNAL TABLE dim_promotion_refer_full
(
    `id`                    STRING COMMENT '营销渠道ID',
    `refer_name`          STRING COMMENT '营销渠道名称',
    `create_time`         STRING COMMENT '创建时间',
    `operate_time`        STRING COMMENT '修改时间'
) COMMENT '营销渠道维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_promotion_refer_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



insert overwrite table dim_promotion_refer_full partition(dt='20250916')
select
    `id`,
    `refer_name`,
    `create_time`,
    `operate_time`
from ods_promotion_refer
where dt=${bizdate};