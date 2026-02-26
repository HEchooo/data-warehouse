# BI数据交接

目前有钱包和电商两套报表系统，由于业务原因，钱包的BI系统基本上处于维护状态
- 电商BI系统 [https://bi.valleysound.xyz/](https://bi.alvinclub.ca/superset/welcome/)
- 钱包BI系统 https://bi.valleysound.xyz/ 你的账号已经加了admin角色
  
## 埋点整体架构
- APP和web上报埋点日志
- 服务端接收埋点上报日志，GCP Storage桶mount到服务器目录，日志写到对应目录
- GCP Storage写入事件触发Eventarc,然后Eventarc触发Cloud Runner函数执行
- Cloud Runner函数读取Storage的日志文件内容，解析日志文件
- 日志内容写入BigQuery表
- 数据分析在SuperSet配置和表格展示

## 一些信息
- superset部署服务器(prod-superset-server 172.30.0.12 已经给你添加了权限)
- Decom埋点日志存储桶:**decom-prod-client-log** (https://console.cloud.google.com/storage/browser/decom-prod-client-log;tab=objects?forceOnBucketsSortingFiltering=true&project=my-project-8584-jetonai&prefix=&forceOnObjectsSortingFiltering=false)
- AC埋点日志存储桶: **mall-prod-client-log**
- EventArc页面 [Triggers](https://console.cloud.google.com/eventarc/triggers?referrer=search&project=my-project-8584-jetonai)
- Decom埋点日志函数 [decom-track-log-runner](https://console.cloud.google.com/run/detail/us-east4/decom-track-log-runner/observability/metrics?project=my-project-8584-jetonai)
- AC埋点日志函数 [mall-track-log-runner](https://console.cloud.google.com/run/detail/us-east4/mall-track-log-runner/observability/metrics?hl=en&project=my-project-8584-jetonai)
- 使用GCP自带DataStream同步 [链接](https://console.cloud.google.com/datastream/streams?referrer=search&project=my-project-8584-jetonai)
- 可以找Max开通下这些角色，方便在GCP的数据工作
- - Artifact Registry Writer
- - BigQuery Admin
- - BigQuery Data Editor
- - BigQuery Job User
- - Cloud Build Editor
- - Cloud Datastore Owner
- - Cloud Run Admin
- - Cloud Run Source Developer
- - Cloud SQL Admin
- - Datastream Admin
- - Eventarc Developer 



## SuperSet信息

```bash
cat /home/echooo/apache-superset/superset_config.py # 查看基础配置
cd apache-superset
. venv/bin/activate


# 开发模式运行
export SUPERSET_SECRET_KEY="bmsBrp5ciix3xRDcGlxUeEpIvuZAB5kqt12IviwiKZDZVgwDdxOgMt8t"
superset run -h 0.0.0.0  -p 8088 --with-threads --reload --debugger


# 后台进程运行
cd apache-superset
. venv/bin/activate  
nohup gunicorn    -w 10     --worker-connections 1000   --timeout 120   -b  0.0.0.0:8088   --limit-request-line 0   --limit-request-field_size 0   "superset.app:create_app()"  &

# 结束进程
pkill gunicorn
```

- SupserSet本身用到数据库部署在devops机器   
- SupserSet超管账号  admin 6MvomReO90Gj 
- SupserSet访问Bigquery是通过Service Account方式连接，可以在“Database connection”看到

数据库账号
```
superset  cUD+I48=LC?nKa3  胡欣使用 
da  ubpi5wnz06!%    胡欣使用  
mysql -h 172.30.0.2 -P 3306 -u superset -p superset
```


##  DataStream同步
- 如果某个表有数据同步失败，可以选择对应表进行“Initiate backfill” 最好是两边总条数做个校验
- Edit -> "Edit source configration" 可以增加同步的表
  
## BigQuery使用经验
- 目前BigQuery计费 按照运行过程数据查询量进行计费
- 大表创建记得选择合适的Partitioning和Clustering  
- 字段变更一般流程，1）基于原表创建变更后临时表,  2） 原表创建快照表进行备份、3） Drop原表、4） 临时表重命名为原表
- 零星的表变更优先用GCP的控制台编辑  
  
字段变更参考
```sql
CREATE OR REPLACE TABLE `decom.cart_item_info_temp`
 PARTITION BY DATE(DATETIME(add_into_cart_time), INTERVAL 4
     HOUR))
        CLUSTER BY device_id, user_id, tenant_code
 AS SELECT * FROM `decom.cart_item_info`;

CREATE SNAPSHOT TABLE `decom.cart_item_info_snapshot`  CLONE `decom.cart_item_info`;

DROP  TABLE `decom.cart_item_info`;

ALTER TABLE `decom.cart_item_info_temp` RENAME TO `cart_item_info`;
```

### 钱包BI
钱包业务BI报表挂载jetonai-bigdata项目下计费
- CloudRun函数bi-data-handler [bi-data-handler](https://console.cloud.google.com/run/detail/asia-southeast1/bi-data-handler/source?project=jetonai-bigdata)
- DataStream同步任务 [wallet-mysql-to-bigquery
](https://console.cloud.google.com/datastream/streams/locations/asia-southeast1/instances/wallet-mysql-to-bigquery;tab=overview?project=jetonai-bigdata)
- EventArc trigger[track-log-create-event-trigger](https://console.cloud.google.com/eventarc/triggers/asia-southeast1/track-log-create-event-trigger?project=jetonai-bigdata)
- GCS Bucket guyin-prod-event-tracking-log [guyin-prod-event-tracking-log](https://console.cloud.google.com/storage/browser/guyin-prod-event-tracking-log?project=jetonai-bigdata)
- 钱包后端问题找Bob组的**冯杰**


#### superset
```bash
cd superset
. venv/bin/activate # 激活虚拟环境
cat  superset_config.py # 查看配置
./server.sh start # 启动服务
./server.sh stop # 停止服务
```
