## magazine, star, brand 映射关系

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
