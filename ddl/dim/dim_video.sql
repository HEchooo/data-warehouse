CREATE TABLE `my-project-8584-jetonai.decom.dim_video` (
    channel STRING,
    creator_id STRING,
    video_id STRING,
    video_key STRING,
    published_ts_utc TIMESTAMP,
    published_dt DATE,
    update_time TIMESTAMP
)
CLUSTER BY channel, creator_id, video_key;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS is_formal_video BOOL;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS formal_tag_created_ts_utc TIMESTAMP;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS video_title STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS video_link STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS account_name STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS account_handle STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS account_url STRING;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS is_social_content_video BOOL;

ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS is_active_account BOOL;

-- 绑定前存量视频：platform_video_tags 的 tag_type='origin' AND tag_value='pre_binding'
ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
ADD COLUMN IF NOT EXISTS is_pre_binding_video BOOL;

-- 绑定前存量视频改在 dim_video 落库时直接排除，不再保留标记字段
ALTER TABLE `my-project-8584-jetonai.decom.dim_video`
DROP COLUMN IF EXISTS is_pre_binding_video;
