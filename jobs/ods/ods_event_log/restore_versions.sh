#!/bin/bash

# 自动版本回退脚本 - 遍历所有文件并恢复到指定版本
# 逻辑：检查每个文件的版本数量，如果是多版本文件则跳过（需要手动处理）

BUCKET="gs://decom-prod-client-log/archived"
DATES=("2025-12-29" "2025-12-30" "2025-12-31" "2026-01-01" "2026-01-02" "2026-01-03" "2026-01-04")
SKIP_FILES=("2025-12-29/220.172.30.0.4.log")  # 需要跳过的文件

echo "========================================"
echo "开始版本回退..."
echo "========================================"

total_restored=0
total_skipped=0

for date in "${DATES[@]}"; do
    echo ""
    echo "--- 处理 $date ---"

    # 获取该日期下所有带版本号的文件
    files=$(gsutil ls -a "$BUCKET/$date/" 2>/dev/null | grep '\.log#' || true)

    if [ -z "$files" ]; then
        echo "  该日期没有日志文件"
        continue
    fi

    while IFS= read -r file_with_version; do
        # 提取不带版本号的文件路径
        file_without_version=$(echo "$file_with_version" | sed 's/#.*//')

        # 提取文件名用于检查是否需要跳过
        filename=$(basename "$file_without_version")
        relative_path="$date/$filename"

        # 检查是否在跳过列表中
        skip=false
        for skip_file in "${SKIP_FILES[@]}"; do
            if [ "$relative_path" = "$skip_file" ]; then
                skip=true
                break
            fi
        done

        if [ "$skip" = true ]; then
            echo "  [跳过] $filename (手动测试文件)"
            ((total_skipped++))
            continue
        fi

        # 检查该文件有多少个版本
        version_count=$(echo "$files" | grep "^$file_without_version#" | wc -l | tr -d ' ')

        if [ "$version_count" -eq 1 ]; then
            # 只有一个版本，直接恢复
            gsutil cp "$file_with_version" "$file_without_version"
            echo "  [成功] $filename (版本号: $(echo "$file_with_version" | cut -d# -f2))"
            ((total_restored++))
        else
            # 多个版本，需要手动处理
            echo "  [需手动] $filename (共 $version_count 个版本)"
            # 显示所有版本供选择
            echo "    可用版本:"
            echo "$files" | grep "^$file_without_version#" | while read -r ver; do
                ver_num=$(echo "$ver" | cut -d# -f2)
                echo "      - $ver_num"
            done
            ((total_skipped++))
        fi
    done <<< "$files"
done

echo ""
echo "========================================"
echo "版本回退完成!"
echo "  成功恢复: $total_restored 个文件"
echo "  跳过/需手动: $total_skipped 个文件"
echo "========================================"
