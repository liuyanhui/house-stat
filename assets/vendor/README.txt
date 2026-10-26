第三方前端依赖（原样入库，永不手改；构建期内联进报告 HTML，运行期零外部请求）

echarts.min.js
  版本:   5.6.0（全量构建，非按需定制包）
  来源:   Apache ECharts 官方发行（Apache License 2.0）
  大小:   1,030,855 bytes
  sha256: e84270bd0cd5bdf60fefc26d00c2a391cb2e81f4d26a7a9ee16185a54773a3cf
  用途:   report/trend_report.html 图表交互（html_render 构建期内联）
          缺失时自动退回纯 PNG 模式（DASHBOARD_NO_JS=1 同效）

升级流程：换新版本时更新本记录的版本号与 sha256，并跑 tests/pixel_diff.py
与 --verify 验收门（JSON option 对新版本 echarts 的兼容性回归）。
