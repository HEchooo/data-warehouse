# 社区互动映射关系

## 适用范围

- 本文档覆盖内容场景里的点赞、关注等服务端互动事实
- 帖子和专栏基础映射见 `docs/rules/post-mapping.md`

## 点赞

- 表：`v3_decom.community_post_like`
- 常用字段：
  - `post_id`
  - `user_id`
  - `gmt_create`

## follow

- 表：`v3_decom.community_star_follow`
- 常用字段：
  - `user_id`
  - `star_user_id`
  - `gmt_create`
- 说明：
  - `star_user_id` 就是专栏 id
  - 不仅包含 `star`，也包含 `magazine`、`brand`
