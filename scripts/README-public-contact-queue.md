# Public Contact Queue Export

这个脚本用于把 `Schedule A` PDF 里的 Amazon 被告名单提取成一个可人工核验的公开联系队列。

边界说明:

- 只导出公开可访问的 `Amazon seller profile` 链接和人工核验字段。
- 不批量抓取邮箱、电话、住址等敏感联系方式。
- 适合律师团队、助理或案件运营同学后续手工核验公开页面。

运行方式:

```bash
cd /Users/serendipitypku/Documents/Playground/tro-case-watch
python3 scripts/export_schedule_a_public_contact_queue.py \
  --pdf "/Users/serendipitypku/Desktop/02255 schedule a.pdf"
```

默认输出到:

- `data/contact-queue/<case-number>-public-contact-queue.csv`
- `data/contact-queue/<case-number>-public-contact-queue.json`
- `data/contact-queue/<case-number>-public-contact-queue.xlsx`
- `data/contact-queue/<case-number>-contact-launcher-mobile.html`

主要字段:

- `doe_no`
- `seller_alias`
- `seller_id`
- `amazon_seller_profile_url`
- `amazon_alias_search_url`
- `public_web_search_url`
- `contact_channel`
- `review_status`
- `notes`

Excel 文件里会额外提供可直接点击的超链接列:

- `打开Amazon店铺页`
- `打开Amazon搜索`
- `打开公开搜索`

HTML 文件是一个适合 iPhone 打开的半自动联系启动台:

- 一键打开当前卖家的 Amazon 店铺页
- 一键复制统一消息模板
- 一键标记“已联系并跳到下一个”
- 在手机浏览器本地保存进度

最新版推荐节奏:

- 点 `复制模板并打开当前店铺`
- 在 Amazon 页面里手工点 `Ask a question -> item -> other`
- 粘贴并发送
- 返回启动台后点 `标记已联系并打开下一个未联系`

如果要让 iPhone 在同一 Wi-Fi 下直接访问，可运行:

```bash
cd /Users/serendipitypku/Documents/Playground/tro-case-watch
bash scripts/serve_public_contact_queue.sh
```
