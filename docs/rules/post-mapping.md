# magazine, star, brand 映射关系

## dataset: v3_decom

- 表 `v3_decom.kol_rel`：
  - `status = 0` 正常
  - `status = 1` 禁用
- 明星 `id / 名称` 映射：
  - `user_id`
  - `nickname`
  - `kol_type = 2`
- 杂志 `id / 名称` 映射：
  - `user_id`
  - `nickname`
  - `kol_type = 3`
- 品牌 `id / 名称` 映射：
  - `user_id`
  - `nickname`
  - `kol_type = 4`

## post_id, post_code 映射关系

- 表 `v3_decom.community_post`
- `post id / post code / post name` 映射：
  - `id`
  - `post_code`
  - `title`

## post_code, column_id 映射关系

- `v3_decom.community_post.post_code`
- `-> v3_decom.community_post.poster_user_id`
- `-> v3_decom.kol_rel.user_id`

## post_code, creator 映射关系

- `v3_decom.community_post.post_code`
- `-> v3_decom.community_post.creator`

## post 创建时间
- `v3_decom.community_post.gmt_create`

## post 状态
- `v3_decom.community_post.status`
- 0-隐藏 
- 1-展示
- 2-审核中

## 试穿帖子
- `json_extract(v3_decom.common_ext_ai_task.request_param,'$.postId')` && `task_type IN ('TRY_ON','STYLE_ME')`

## 创建模特
- `v3_decom.common_ext_ai_task.task_type = 'MODEL'`

## 试穿人数
- `v3_decom.common_ext_ai_task.request_param.user_id` && `task_type IN ('TRY_ON','STYLE_ME')`

## 点赞
- `v3_decom.community_post_like.post_id`, `v3_decom.community_post_like.user_id`, `v3_decom.community_post_like.gmt_create`

## follow
- `v3_decom.community_star_follow.user_id`, `v3_decom.community_star_follow.star_user_id`, `v3_decom.community_star_follow.gmt_create` 
- star_user_id 就是专栏 id，不仅是 star 也包含 magazine, brand
