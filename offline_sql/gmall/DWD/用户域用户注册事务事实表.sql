DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_user_register_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_user_register_inc
(
    `user_id`          STRING COMMENT '用户ID',
    `date_id`          STRING COMMENT '日期ID',
    `create_time`     STRING COMMENT '注册时间',
    `channel`          STRING COMMENT '应用下载渠道',
    `province_id`     STRING COMMENT '省份ID',
    `version_code`    STRING COMMENT '应用版本',
    `mid_id`           STRING COMMENT '设备ID',
    `brand`            STRING COMMENT '设备品牌',
    `model`            STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_user_register_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_user_register_inc partition(ds = '20250916')
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        id user_id,  -- 直接使用独立字段id，替代data.id
        create_time  -- 直接使用独立字段create_time，替代data.create_time
    from bigdata_offline_v1_ws.ods_user_info  -- 表名改为ods_user_info
    where dt=${bizdate} -- 无type字段，按分区筛选首日全量注册数据
)ui
left join
(
    select
        get_json_object(log, '$.common.uid') as user_id,
        get_json_object(log, '$.common.ch') as channel,
        get_json_object(log, '$.common.ar') as province_id,
        get_json_object(log, '$.common.vc') as version_code,
        get_json_object(log, '$.common.mid') as mid_id,
        get_json_object(log, '$.common.ba') as brand,
        get_json_object(log, '$.common.md') as model,
        get_json_object(log, '$.common.os') as operate_system
    from bigdata_offline_v1_ws.ods_z_log
    where dt=${bizdate}
      and get_json_object(log, '$.page.page_id')='register'
      and get_json_object(log, '$.common.uid') is not null
)log
on ui.user_id = log.user_id;


--10. 用户域用户登录事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_user_login_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_user_login_inc
(
    `user_id`         STRING COMMENT '用户ID',
    `date_id`         STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`         STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`          STRING COMMENT '设备ID',
    `brand`           STRING COMMENT '设备品牌',
    `model`           STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_user_login_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

insert overwrite table bigdata_offline_v1_ws.dwd_user_login_inc partition (ds = '20250916')
select
    get_json_object(log, '$.common.uid') as user_id,
    -- 解析ts转换为日期ID
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd') date_id,
    -- 解析ts转换为登录时间
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
    -- 解析log字段中的渠道、设备等信息
    get_json_object(log, '$.common.ch') channel,
    get_json_object(log, '$.common.ar') province_id,
    get_json_object(log, '$.common.vc') version_code,
    get_json_object(log, '$.common.mid') mid_id,
    get_json_object(log, '$.common.ba') brand,
    get_json_object(log, '$.common.md') model,
    get_json_object(log, '$.common.os') operate_system
from (
         select
             log,
             -- 按会话ID分区，取每个会话首条登录记录
             row_number() over (partition by get_json_object(log, '$.common.sid') order by cast(get_json_object(log, '$.ts') as bigint)) rn
         from bigdata_offline_v1_ws.ods_z_log
         where dt=${bizdate}
           and get_json_object(log, '$.page') is not null
           and get_json_object(log, '$.common.uid') is not null
     ) t
where t.rn = 1;