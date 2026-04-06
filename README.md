# Guide Merger

自动合并多个EPG源的频道节目信息

## 功能特性

这个完整脚本具备以下功能：

✅ 支持HTTP/HTTPS - 自动处理SSL证书

✅ 支持压缩文件 - 自动解压 .gz 格式

✅ 源级别别名映射 - 在 source_guide.txt 中直接定义映射

✅ 智能排序 - 数字 → 字母（不区分大小写） → 汉字（拼音）

✅ 自动去重 - 频道和节目基于最终ID去重

✅ 时间范围过滤 - 只保留指定时间内的节目

✅ 双格式输出 - 同时生成 .xml 和 .xml.gz

✅ 重试机制 - 下载失败自动重试

✅ 详细日志 - 显示处理进度和统计信息

✅ GitHub Actions集成 - 自动定时更新

## 使用方法

1. 编辑 `source_guide.txt` 配置文件
2. 运行脚本：`python guide_merger.py`
3. 生成的 `guide.xml` 即为合并后的EPG文件

## 配置文件格式

###### `source_guide.txt` 配置示例

Timeframe 指定Guide包含的小时数

###### 实际效果示例	

```
timeframe=48 配置
      过去24小时               未来24小时
←──────────────────────┬───────────────────→
4月2日 12:00      4月3日 12:00      4月4日 12:00
     ↑                 ↑                 ↑
  开始边界           当前时间            结束边界		
		
```

EPG源参数：

**Timezone**：

- **有 TimeZone 参数**：转换到北京时间（当时区是 +8 时区（无论何种格式，如 `+0800`、`+8`、`UTC+8`、`GMT+8` 等）时，不进行转换，直接保持原样）

- **没有 TimeZone 参数**：保持原时区不变

  Timezone支持的时区格式：

| 格式   | 示例         | 说明         |
| ------ | ------------ | ------------ |
| ±HHMM  | +0800, -0500 | 标准时区格式 |
| ±HH    | +8, -5       | 简写格式     |
| UTC±HH | UTC+8, UTC-5 | 带UTC前缀    |
| GMT±HH | GMT+8, GMT-5 | 带GMT前缀    |

**ChangeTimezone**

ChangeTimezone=Y 强制转换时区为+8时区

ChangeTimezone=N或无 不强制转换为+8时区，保持原状 



示例：		


```
timeframe=96

# 源1：UTC时间的源（需要转换为北京时间）
https://example.com/guide.xml
TimeZone=+0000
1	CCTV1
2	CCTV2
明珠台
BBC Earth

# 源2：已经是北京时间的源（不需要转换）
https://example.com/guide.xml
TimeZone=+0800
CCTV1
CCTV2
CCTV3
CCTV4

# 源3：美国东部时间的源
https://example.com/guide.xml
TimeZone=-0500
ChangeTimezone=Y
HBO
CNN
FOX

# 源4：不指定时区，默认使用北京时间
https://example.com/guide.xml
CCTV1
CCTV2
CCTV3
```



