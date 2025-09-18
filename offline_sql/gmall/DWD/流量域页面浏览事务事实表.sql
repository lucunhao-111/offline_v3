DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`       STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页ID',
    `page_id`          STRING COMMENT '页面ID ',
    `from_pos_id`     STRING COMMENT '点击坑位ID',
    `from_pos_seq`    STRING COMMENT '点击坑位位置',
    `refer_id`         STRING COMMENT '营销渠道ID',
    `date_id`          STRING COMMENT '日期ID',
    `view_time`       STRING COMMENT '跳入时间',
    `session_id`      STRING COMMENT '所属会话ID',
    `during_time`     BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


set hive.cbo.enable=false;
insert overwrite table bigdata_offline_v1_ws.dwd_traffic_page_view_inc partition (dt='20250918')
select
    -- 从log字段解析common结构体中的ar（省份ID）
    get_json_object(log, '$.common.ar') province_id,
    -- 解析common结构体中的ba（手机品牌）
    get_json_object(log, '$.common.ba') brand,
    -- 解析common结构体中的ch（渠道）
    get_json_object(log, '$.common.ch') channel,
    -- 解析common结构体中的is_new（是否首次启动）
    get_json_object(log, '$.common.is_new') is_new,
    -- 解析common结构体中的md（手机型号）
    get_json_object(log, '$.common.md') model,
    -- 解析common结构体中的mid（设备ID）
    get_json_object(log, '$.common.mid') mid_id,
    -- 解析common结构体中的os（操作系统）
    get_json_object(log, '$.common.os') operate_system,
    -- 解析common结构体中的uid（用户ID）
    get_json_object(log, '$.common.uid') user_id,
    -- 解析common结构体中的vc（APP版本号）
    get_json_object(log, '$.common.vc') version_code,
    -- 解析page结构体中的item（目标ID）
    get_json_object(log, '$.page.item') page_item,
    -- 解析page结构体中的item_type（目标类型）
    get_json_object(log, '$.page.item_type') page_item_type,
    -- 解析page结构体中的last_page_id（上页ID）
    get_json_object(log, '$.page.last_page_id') last_page_id,
    -- 解析page结构体中的page_id（当前页面ID）
    get_json_object(log, '$.page.page_id') page_id,
    -- 解析page结构体中的from_pos_id（点击坑位ID）
    get_json_object(log, '$.page.from_pos_id') from_pos_id,
    -- 解析page结构体中的from_pos_seq（点击坑位位置）
    get_json_object(log, '$.page.from_pos_seq') from_pos_seq,
    -- 解析page结构体中的refer_id（营销渠道ID）
    get_json_object(log, '$.page.refer_id') refer_id,
    -- 解析ts（时间戳）并转换为日期ID（yyyy-MM-dd）
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd') date_id,
    -- 解析ts并转换为完整时间（yyyy-MM-dd HH:mm:ss）
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') view_time,
    -- 解析common结构体中的sid（会话ID）
    get_json_object(log, '$.common.sid') session_id,
    -- 解析page结构体中的during_time（持续时间，毫秒）
    cast(get_json_object(log, '$.page.during_time') as bigint) during_time
from bigdata_offline_v1_ws.ods_z_log
-- 筛选条件：分区为当日，且log字段中的page结构体不为null（确保是页面浏览日志）
where dt=${bizdate}
  and get_json_object(log, '$.page') is not null;
set hive.cbo.enable=true;





