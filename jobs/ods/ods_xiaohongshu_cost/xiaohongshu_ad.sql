CREATE TABLE xiaohongshu_ad
(
    date DATETIME,
    country_name VARCHAR(255),
    fee FLOAT,
    impression INT,
    click INT
)
DISTRIBUTED BY HASH(date) BUCKETS 32;




