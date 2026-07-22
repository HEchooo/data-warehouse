CREATE TABLE `my-project-8584-jetonai.decom.ads_daily_lta_spu` (
    dt DATE,
    spu STRING,
    spu_name STRING,
    kol_count INT64,
    goods_exposure_pv INT64,
    goods_exposure_uv INT64,
    goods_click_pv INT64,
    goods_click_uv INT64,
    goods_click_cvr_pv FLOAT64,
    goods_click_cvr_uv FLOAT64,
    update_time TIMESTAMP
)
PARTITION BY dt
CLUSTER BY spu;
