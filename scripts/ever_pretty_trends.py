"""本地 Ever Pretty 趋势看板；不创建或改写 BigQuery 数据。"""
from copy import deepcopy
import json


def configure_trends(api, upsert, ds, dashboard, preview, time_range, max_dt, runtime):
    api('PUT', f'/api/v1/dataset/{ds}/refresh')
    dataset = api('GET', f'/api/v1/dataset/{ds}')['result']
    old_label = next((c for c in dataset['columns'] if c['column_name'] == 'video_label'), None)
    label = {'column_name': 'video_label', 'verbose_name': '视频', 'expression': "CONCAT(blogger_name, ' · ', COALESCE(SUBSTR(video_title, 1, 65), '待平台回流'), ' [', video_id, ']')", 'type': 'STRING', 'filterable': True, 'groupby': True, 'is_dttm': False}
    if old_label:
        label['id'] = old_label['id']
    columns = [{k: c[k] for k in ('id', 'column_name', 'verbose_name', 'expression', 'type', 'filterable', 'groupby', 'is_dttm') if k in c} for c in dataset['columns'] if c['column_name'] != 'video_label']
    api('PUT', f'/api/v1/dataset/{ds}', {'columns': columns + [label]})
    refreshed = api('GET', f'/api/v1/dataset/{ds}')['result']
    assert len(refreshed['columns']) >= len(dataset['columns'])

    start_date = preview['config']['start_date']
    known = len({r['video_key'] for r in preview['rows'] if r['published_time_basis'] == '平台发布时间'})
    pending = len({r['video_key'] for r in preview['rows'] if r['published_time_basis'] != '平台发布时间'})
    definitions = [
        ('views', '播放趋势 · 每日已知累计', 'echarts_timeseries_line', 'SUM(total_views)', '累计播放', 12, 44),
        ('growth', '播放变化 · 相邻记录增量', 'echarts_timeseries_bar', 'SUM(IF(has_snapshot, view_increment, NULL))', '播放记录增量', 8, 34),
        ('coverage', '数据新鲜度 · 当天采集视频数', 'echarts_timeseries_bar', 'COUNTIF(has_snapshot)', '当天采集视频数', 4, 34),
        ('likes', '点赞趋势', 'echarts_timeseries_line', 'SUM(total_likes)', '累计点赞', 6, 34),
        ('comments', '评论趋势', 'echarts_timeseries_line', 'SUM(total_comments)', '累计评论', 6, 34),
    ]
    charts = {}
    contexts = {}
    desc = f'统计日期为美东时间。仅覆盖{start_date}起发布的视频；累计值按同一天汇总，不跨日相加。缺采时累计沿用旧值，首次采集前未知；增量仅展示有采集且有比较基线的记录变化，不等同于真实当日新增。'
    for key, title, viz, sql, metric_label, width, height in definitions:
        metric = {'expressionType': 'SQL', 'sqlExpression': sql, 'label': metric_label, 'optionName': 'metric_' + key}
        form = {'datasource': f'{ds}__table', 'viz_type': viz, 'x_axis': 'dt', 'granularity_sqla': 'dt', 'time_grain_sqla': 'P1D', 'time_range': time_range,
            'metrics': [metric], 'groupby': [], 'adhoc_filters': [], 'row_limit': 10000, 'series_limit': 0,
            'order_desc': False, 'show_legend': False, 'showTooltipTotal': False, 'rich_tooltip': True,
            'markerEnabled': True, 'markerSize': 5, 'lineInterpolation': 'linear', 'show_value': False,
            'x_axis_time_format': '%m/%d', 'y_axis_format': ',d', 'y_axis_title': metric_label,
            'y_axis_title_margin': 20, 'y_axis_title_position': 'Top', 'truncateYAxis': False,
            'color_scheme': 'supersetColors', 'zoomable': True, 'dashboards': [dashboard]}
        query = {'time_range': time_range, 'granularity': 'dt', 'is_timeseries': True, 'columns': [], 'metrics': [metric], 'filters': [],
            'orderby': [], 'row_limit': 10000, 'extras': {'time_grain_sqla': 'P1D', 'having': '', 'where': ''}}
        context = {'datasource': {'id': ds, 'type': 'table'}, 'force': False, 'queries': [query], 'form_data': form, 'result_format': 'json', 'result_type': 'full'}
        ident = upsert('chart', {'slice_name': title, 'viz_type': viz, 'datasource_id': ds, 'datasource_type': 'table', 'dashboards': [dashboard],
            'description': desc, 'params': json.dumps(form, ensure_ascii=False), 'query_context': json.dumps(context, ensure_ascii=False)}, lambda r, name=title: r['slice_name'] == name)
        charts[key] = ident
        contexts[key] = context

    ids = list(charts.values())
    filters = [{'id': 'NATIVE_FILTER-trend-date', 'name': '日期（美东）', 'filterType': 'filter_time', 'targets': [{}],
        'defaultDataMask': {'extraFormData': {'time_range': time_range}, 'filterState': {'value': time_range}},
        'scope': {'rootPath': ['ROOT_ID'], 'excluded': []}, 'chartsInScope': ids, 'type': 'NATIVE_FILTER', 'cascadeParentIds': []}]
    for field, name, parents in [('blogger_name', '博主 · 可选', []), ('channel', '平台 · 可选', ['blogger_name']), ('video_label', '视频 · 可搜索标题', ['blogger_name', 'channel'])]:
        filters.append({'id': 'NATIVE_FILTER-trend-' + field, 'name': name, 'filterType': 'filter_select', 'targets': [{'datasetId': ds, 'column': {'name': field}}],
            'defaultDataMask': {'extraFormData': {}, 'filterState': {}},
            'controlValues': {'multiSelect': False, 'searchAllOptions': True, 'enableEmptyFilter': False, 'defaultToFirstItem': False},
            'scope': {'rootPath': ['ROOT_ID'], 'excluded': []}, 'chartsInScope': ids, 'type': 'NATIVE_FILTER',
            'cascadeParentIds': ['NATIVE_FILTER-trend-' + p for p in parents]})

    pos = {'DASHBOARD_VERSION_KEY': 'v2', 'ROOT_ID': {'id': 'ROOT_ID', 'type': 'ROOT', 'children': ['GRID_ID']},
        'GRID_ID': {'id': 'GRID_ID', 'type': 'GRID', 'parents': ['ROOT_ID'], 'children': []},
        'HEADER_ID': {'id': 'HEADER_ID', 'type': 'HEADER', 'meta': {'text': 'Ever Pretty · 视频表现'}}}
    def row(name):
        ident = 'ROW-' + name
        pos['GRID_ID']['children'].append(ident)
        pos[ident] = {'id': ident, 'type': 'ROW', 'parents': ['ROOT_ID', 'GRID_ID'], 'children': [], 'meta': {'background': 'BACKGROUND_TRANSPARENT'}}
        return ident
    def markdown(name, text, height):
        r = row(name); ident = 'MARKDOWN-' + name
        pos[r]['children'].append(ident)
        pos[ident] = {'id': ident, 'type': 'MARKDOWN', 'parents': ['ROOT_ID', 'GRID_ID', r], 'children': [], 'meta': {'code': text, 'width': 12, 'height': height}}
    markdown('intro', f'### 视频表现，一眼看趋势\n\n默认查看整体；选择 **博主 → 视频**，即可聚焦单条视频。　数据截至 **{max_dt}（美东）** · 发布范围自 {start_date} 起', 12)
    for row_name, keys in [('hero', ['views']), ('change', ['growth', 'coverage']), ('engagement', ['likes', 'comments'])]:
        r = row(row_name)
        for key in keys:
            _, title, _, _, _, width, height = next(d for d in definitions if d[0] == key)
            ident = 'CHART-' + key
            pos[r]['children'].append(ident)
            pos[ident] = {'id': ident, 'type': 'CHART', 'parents': ['ROOT_ID', 'GRID_ID', r], 'children': [], 'meta': {'chartId': charts[key], 'sliceName': title, 'width': width, 'height': height}}
    markdown('notes', f'**读图说明**　累计曲线包含沿用值；无采集的日期不显示增量柱。跨缺采日期的增长会落在下一次采集记录上，不代表全部发生在当天。首条记录没有增量基线。\n\n**数据覆盖**　{known} 个已回流视频，另有 {pending} 个待回流占位；UnfilteredMomFinds 的 TikTok 尚无可关联视频。YouTube 不提供分享数，因此本页不汇总跨平台分享。Demo 尚未启用定时刷新。', 16)
    existing = api('GET', f'/api/v1/dashboard/{dashboard}')['result']
    metadata = json.loads(existing.get('json_metadata') or '{}')
    metadata.update({'native_filter_configuration': filters, 'show_native_filters': True, 'filter_bar_orientation': 'VERTICAL', 'expanded_slices': {}, 'chart_configuration': {}})
    api('PUT', f'/api/v1/dashboard/{dashboard}', {'dashboard_title': 'Ever Pretty · 视频表现', 'position_json': json.dumps(pos, ensure_ascii=False),
        'json_metadata': json.dumps(metadata, ensure_ascii=False), 'css': '.dashboard-content { background: #f6f8fb; } .dashboard-component-chart-holder { border-radius: 12px; }'})
    # 旧明细图保留资源，只从本看板移除。
    for c in api('GET', f'/api/v1/dashboard/{dashboard}/charts')['result']:
        if c['id'] not in ids:
            detail = api('GET', f"/api/v1/chart/{c['id']}")['result']
            remaining = [d['id'] for d in detail.get('dashboards', []) if d['id'] != dashboard]
            api('PUT', f"/api/v1/chart/{c['id']}", {'dashboards': remaining})
    actual = api('GET', f'/api/v1/dashboard/{dashboard}/charts')['result']
    assert set(c['id'] for c in actual) == set(ids)
    assert api('GET', f'/api/v1/dashboard/{dashboard}')['result'].get('roles') == existing.get('roles')
    results = {}
    for key, context in contexts.items():
        q = deepcopy(context); q['force'] = True
        result = api('POST', '/api/v1/chart/data', q)['result'][0]
        assert not result.get('error') and result.get('data'), (key, result.get('error'))
        results[key] = result
    # 单视频过滤后的播放曲线必须与逐日底表一致。
    q = deepcopy(contexts['views']); q['force'] = True
    q['queries'][0]['filters'] = [{'col': 'video_id', 'op': '==', 'val': 'GxIfNEHg1MQ'}]
    single = api('POST', '/api/v1/chart/data', q)['result'][0]
    expected = sorted((r for r in preview['rows'] if r['video_id'] == 'GxIfNEHg1MQ'), key=lambda r:r['dt'])
    got = sorted(single['data'], key=lambda r:r['__timestamp'])
    assert [r['累计播放'] for r in got] == [r['total_views'] for r in expected]
    (runtime / 'trend_results.json').write_text(json.dumps(results, ensure_ascii=False, default=str))
    (runtime / 'superset_ids.json').write_text(json.dumps({'dataset': ds, 'dashboard': dashboard, 'charts': charts}))
    print(json.dumps({'charts': charts, 'daily_points': {k:len(v['data']) for k,v in results.items()}, 'single_video_days_verified': len(got)}, ensure_ascii=False))
