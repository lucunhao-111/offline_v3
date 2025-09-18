DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



insert overwrite table dim_user_zip partition (dt = '99991231')
select id,
       concat(substr(name, 1, 1), '*') name,
       if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(phone_num, 1, 3), '*'), null) phone_num,
       if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(email, '@')[1]), null)   email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       '20250916'start_date,
       '99991231'end_date
from ods_user_info
where dt = ${bizdate};

select * from dim_activity_full;