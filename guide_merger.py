#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG Merger Script - 合并多个EPG源的频道节目信息
支持 .xml 和 .xml.gz 格式
支持在 source_guide.txt 中直接定义频道别名映射
支持智能排序（按display-name，数字-字母-汉字，不区分大小写）
支持每个EPG源单独设置时区转换（可选，不设置则保持原时区）
支持前后双向时间范围（包含过去和未来的节目，按天对齐）
可配置是否修改 channel id 和 display-name
支持跨天节目拆分（如 23:40-0:20 拆分为两个节目）
支持智能合并：对于相同时间的节目，保留信息更完整的版本
支持 curl_cffi 模拟真实浏览器 TLS 指纹（绕过反爬）
"""

import gzip
import xml.etree.ElementTree as ET
import os
import sys
import time
import re
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import hashlib
import copy
from urllib.parse import urlparse

# 尝试导入 curl_cffi（浏览器指纹模拟）
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    import requests
    print("⚠ 未安装 curl_cffi 库，将使用普通 requests")
    print("  安装方法: pip install curl-cffi")

# 尝试导入拼音转换库（用于中文排序）
try:
    from pypinyin import pinyin, Style
    HAS_PYPINYIN = True
except ImportError:
    HAS_PYPINYIN = False
    print("⚠ 未安装pypinyin库，中文将按Unicode排序")
    print("  安装方法: pip install pypinyin")

# ==================== 配置常量 ====================
SOURCE_FILE = 'source_guide.txt'         # EPG源配置文件
OUTPUT_XML = 'guide.xml'                 # 输出XML文件名
OUTPUT_GZ = 'guide.xml.gz'               # 输出GZ压缩文件名
TEMP_DIR_NAME = 'temp_epg_files'         # 临时文件目录
COOKIE_DIR = 'cookies'                   # Cookie 保存目录
DEFAULT_PAST_DAYS = 6                    # 默认过去天数（6天）
DEFAULT_FUTURE_DAYS = 3                  # 默认未来天数（3天）
MAX_RETRIES = 3                          # 最大重试次数
DOWNLOAD_TIMEOUT = 30                    # 下载超时（秒）
CHUNK_SIZE = 131072                      # 下载块大小（128KB）

# ==================== 别名映射配置 ====================
MODIFY_CHANNEL_ID = True      # True: 修改channel id, False: 不修改
MODIFY_DISPLAY_NAME = True    # True: 修改display-name, False: 不修改

# ==================== 跨天节目拆分配置 ====================
SPLIT_OVERNIGHT_PROGRAMS = True   # True: 拆分跨天节目, False: 保持原样

# ==================== 智能合并配置 ====================
SMART_MERGE = True   # True: 启用智能合并（保留信息更完整的节目）, False: 禁用

# ==================== 浏览器模拟配置 ====================
# 可选的浏览器指纹: 'chrome110', 'chrome120', 'safari15_5', 'edge101'
BROWSER_IMPERSONATE = 'chrome120'

# ==================== 时区配置 ====================
BEIJING_TZ = timezone(timedelta(hours=8))  # 北京时区 UTC+8
UTC = timezone.utc                         # UTC时区

# 时区映射表
TIMEZONE_MAP = {
    '+0000': timezone(timedelta(hours=0)),
    '+0100': timezone(timedelta(hours=1)),
    '+0200': timezone(timedelta(hours=2)),
    '+0300': timezone(timedelta(hours=3)),
    '+0400': timezone(timedelta(hours=4)),
    '+0500': timezone(timedelta(hours=5)),
    '+0600': timezone(timedelta(hours=6)),
    '+0700': timezone(timedelta(hours=7)),
    '+0800': timezone(timedelta(hours=8)),
    '+0900': timezone(timedelta(hours=9)),
    '+1000': timezone(timedelta(hours=10)),
    '+1100': timezone(timedelta(hours=11)),
    '+1200': timezone(timedelta(hours=12)),
    '-0100': timezone(timedelta(hours=-1)),
    '-0200': timezone(timedelta(hours=-2)),
    '-0300': timezone(timedelta(hours=-3)),
    '-0400': timezone(timedelta(hours=-4)),
    '-0500': timezone(timedelta(hours=-5)),
    '-0600': timezone(timedelta(hours=-6)),
    '-0700': timezone(timedelta(hours=-7)),
    '-0800': timezone(timedelta(hours=-8)),
    '-0900': timezone(timedelta(hours=-9)),
    '-1000': timezone(timedelta(hours=-10)),
    '-1100': timezone(timedelta(hours=-11)),
    '-1200': timezone(timedelta(hours=-12)),
}


# ==================== 工具函数 ====================
def print_separator(char: str = '=', length: int = 60) -> None:
    """打印分隔线"""
    print(char * length)


def format_size(bytes_size: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def compress_gzip(input_file: str, output_file: str) -> bool:
    """压缩文件为gzip格式"""
    try:
        with open(input_file, 'rb') as f_in:
            with gzip.open(output_file, 'wb', compresslevel=9) as f_out:
                f_out.write(f_in.read())
        
        original_size = os.path.getsize(input_file)
        compressed_size = os.path.getsize(output_file)
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        print(f'  ✅ 压缩完成: {format_size(compressed_size)} ({compression_ratio:.1f}% 压缩率)')
        return True
    except Exception as e:
        print(f'  ❌ 压缩失败: {e}')
        return False


def get_cookie_file(url: str) -> str:
    """获取域名对应的 Cookie 文件路径"""
    parsed = urlparse(url)
    domain = parsed.netloc.replace(':', '_')
    os.makedirs(COOKIE_DIR, exist_ok=True)
    return os.path.join(COOKIE_DIR, f"{domain}.cookiejar")


def is_beijing_timezone(timezone_str: str) -> bool:
    """判断时区是否为北京时间（+8时区）"""
    if not timezone_str:
        return False
    
    tz_upper = timezone_str.strip().upper()
    
    if tz_upper in ['+8', '+0800', '8', '0800']:
        return True
    
    if 'UTC+8' in tz_upper or 'GMT+8' in tz_upper:
        return True
    
    if re.search(r'UTC[+]0?8', tz_upper) or re.search(r'GMT[+]0?8', tz_upper):
        return True
    
    if tz_upper == '+08:00':
        return True
    
    return False


def parse_timezone(timezone_str: str) -> Optional[timezone]:
    """解析时区字符串，返回timezone对象"""
    if not timezone_str:
        return None
    
    if is_beijing_timezone(timezone_str):
        print(f'    ✅ 检测到北京时间 (+8时区)，将保持原样不转换')
        return None
    
    tz_upper = timezone_str.strip().upper()
    
    if 'UTC' in tz_upper or 'GMT' in tz_upper:
        match = re.search(r'([+-])(\d+)', tz_upper)
        if match:
            sign = match.group(1)
            hours = int(match.group(2))
            if sign == '-':
                hours = -hours
            return timezone(timedelta(hours=hours))
    
    if tz_upper in TIMEZONE_MAP:
        return TIMEZONE_MAP[tz_upper]
    
    match = re.match(r'^([+-])(\d+)$', tz_upper)
    if match:
        sign = match.group(1)
        hours = int(match.group(2))
        if sign == '-':
            hours = -hours
        return timezone(timedelta(hours=hours))
    
    print(f'    ⚠ 无法识别的时区: {timezone_str}，将保持原时区不变')
    return None


def extract_timezone_from_time_str(time_str: str) -> Optional[timezone]:
    """从时间字符串中提取时区信息"""
    if not time_str:
        return None
    
    try:
        if ' +' in time_str or ' -' in time_str:
            parts = time_str.split()
            if len(parts) >= 2:
                tz_str = parts[1]
                if tz_str in TIMEZONE_MAP:
                    return TIMEZONE_MAP[tz_str]
        return None
    except Exception:
        return None


def convert_timezone(time_str: str, source_tz: timezone, target_tz: timezone) -> str:
    """将时间字符串从源时区转换为目标时区（时间数值会变化）"""
    if not time_str or source_tz is None or target_tz is None:
        return time_str
    
    try:
        if ' +' in time_str or ' -' in time_str:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M%S %z')
        else:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M%S')
            dt = dt.replace(tzinfo=source_tz)
        
        dt_target = dt.astimezone(target_tz)
        return dt_target.strftime('%Y%m%d%H%M%S %z')
    except Exception:
        return time_str


def change_timezone_only(time_str: str, target_tz_str: str = '+0800') -> str:
    """仅修改时间字符串的时区后缀，不改变时间数值"""
    if not time_str:
        return time_str
    
    try:
        if ' +' in time_str or ' -' in time_str:
            time_part = time_str.split()[0]
        else:
            time_part = time_str
        
        return f"{time_part} {target_tz_str}"
    except Exception:
        return time_str


def parse_datetime_from_str(time_str: str) -> Optional[datetime]:
    """从时间字符串解析datetime对象（带时区）"""
    if not time_str:
        return None
    
    try:
        if ' +' in time_str or ' -' in time_str:
            return datetime.strptime(time_str, '%Y%m%d%H%M%S %z')
        else:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M%S')
            return dt.replace(tzinfo=BEIJING_TZ)
    except Exception:
        return None


def is_overnight_program(start_dt: datetime, stop_dt: datetime) -> bool:
    """判断节目是否跨天（结束日期 > 开始日期）"""
    if not start_dt or not stop_dt:
        return False
    return stop_dt.date() > start_dt.date()


def get_program_completeness_score(programme: ET.Element) -> int:
    """计算节目信息的完整度分数（分数越高越完整）"""
    score = 0
    
    if programme.find('title') is not None:
        score += 10
    if programme.find('desc') is not None:
        score += 5
    if programme.find('sub-title') is not None:
        score += 3
    if programme.find('category') is not None:
        score += 2
    if programme.find('icon') is not None:
        score += 1
    if programme.find('credits') is not None:
        score += 1
    if programme.find('length') is not None:
        score += 1
    if programme.find('rating') is not None:
        score += 1
    if programme.find('video') is not None:
        score += 1
    if programme.find('audio') is not None:
        score += 1
    
    return score


def is_programme_more_complete(prog1: ET.Element, prog2: ET.Element) -> bool:
    """判断节目1是否比节目2更完整"""
    score1 = get_program_completeness_score(prog1)
    score2 = get_program_completeness_score(prog2)
    
    if score1 > score2:
        return True
    elif score1 < score2:
        return False
    else:
        text1 = ET.tostring(prog1, encoding='unicode')
        text2 = ET.tostring(prog2, encoding='unicode')
        return len(text1) > len(text2)


def merge_programmes(existing: ET.Element, new: ET.Element) -> ET.Element:
    """合并两个节目，保留更完整的信息"""
    tags_to_merge = ['title', 'desc', 'sub-title', 'category', 'icon', 'credits', 'length', 'rating', 'video', 'audio']
    
    for tag in tags_to_merge:
        existing_elem = existing.find(tag)
        new_elem = new.find(tag)
        
        if existing_elem is None and new_elem is not None:
            new_child = copy.deepcopy(new_elem)
            existing.append(new_child)
    
    return existing


def split_overnight_program(
    programme: ET.Element,     # 原始节目XML元素
    start_dt: datetime,               # 节目实际开始时间
    stop_dt: datetime,               # 节目实际结束时间
    channel_id: str                     # 频道ID
) -> List[ET.Element]:              # 返回拆分后的两个节目元素
    """将跨天节目拆分为两个节目"""
    end_of_day = start_dt.replace(hour=23, minute=59, second=59)      # 计算拆分时间点,当天23:59:59
    next_day_start = stop_dt.replace(hour=0, minute=0, second=0)       # 第二天00:00:00
    
    part1_start = start_dt
    part1_stop = end_of_day
    
    part2_start = next_day_start
    part2_stop = stop_dt
    
    part1 = copy.deepcopy(programme)
    part1.set('start', part1_start.strftime('%Y%m%d%H%M%S %z'))
    part1.set('stop', part1_stop.strftime('%Y%m%d%H%M%S %z'))
    if channel_id:
        part1.set('channel', channel_id)
    
    part2 = copy.deepcopy(programme)
    part2.set('start', part2_start.strftime('%Y%m%d%H%M%S %z'))
    part2.set('stop', part2_stop.strftime('%Y%m%d%H%M%S %z'))
    if channel_id:
        part2.set('channel', channel_id)
    
    return [part1, part2]


def convert_date_for_filter(time_str: str, source_tz: timezone) -> Optional[datetime]:
    """将时间字符串转换为UTC datetime对象（用于时间范围过滤）"""
    if not time_str:
        return None
    
    try:
        if ' +' in time_str or ' -' in time_str:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M%S %z')
        else:
            dt = datetime.strptime(time_str, '%Y%m%d%H%M%S')
            if source_tz:
                dt = dt.replace(tzinfo=source_tz)
            else:
                dt = dt.replace(tzinfo=UTC)
        
        return dt.astimezone(UTC)
    except Exception:
        return None


# ==================== 智能排序函数（按display-name）====================
def get_display_name(channel: ET.Element) -> str:
    """获取频道的显示名称"""
    display_name = channel.find('display-name')
    if display_name is not None and display_name.text:
        return display_name.text.strip()
    return channel.attrib.get('id', '')


def get_sort_key_by_display(channel_name: str) -> Tuple[int, str, str]:
    """生成排序键，实现：数字 → 字母 → 汉字 的排序"""
    if not channel_name:
        return (3, '', '')
    
    first_char = channel_name[0]
    
    if first_char.isdigit():
        match = re.match(r'^(\d+)', channel_name)
        if match:
            num = int(match.group(1))
            remaining = channel_name[len(match.group(1)):].lower()
            return (0, f"{num:010d}", remaining)
        return (0, channel_name.lower(), channel_name)
    
    elif first_char.isalpha() and first_char.isascii():
        return (1, channel_name.lower(), channel_name)
    
    elif '\u4e00' <= first_char <= '\u9fff':
        if HAS_PYPINYIN:
            try:
                pinyin_list = pinyin(channel_name, style=Style.NORMAL)
                pinyin_str = ''.join([p[0].lower() for p in pinyin_list])
                return (2, pinyin_str, channel_name)
            except:
                return (2, channel_name, channel_name)
        else:
            return (2, channel_name, channel_name)
    
    else:
        return (3, channel_name.lower(), channel_name)


def sort_channels_by_display(channels: List[ET.Element]) -> List[ET.Element]:
    """智能排序频道列表（按display-name）"""
    def channel_key(channel):
        display_name = get_display_name(channel)
        return get_sort_key_by_display(display_name)
    
    return sorted(channels, key=channel_key)


def sort_programmes_by_display(programmes: List[ET.Element], 
                                channel_dict: Dict[str, ET.Element]) -> List[ET.Element]:
    """智能排序节目列表（按频道的display-name排序）"""
    channel_display_map = {}
    for channel_id, channel in channel_dict.items():
        channel_display_map[channel_id] = get_display_name(channel)
    
    def programme_key(programme):
        channel_id = programme.attrib.get('channel', '')
        display_name = channel_display_map.get(channel_id, channel_id)
        start_time = programme.attrib.get('start', '')
        return (get_sort_key_by_display(display_name), start_time)
    
    return sorted(programmes, key=programme_key)


# ==================== 应用别名映射到频道 ====================
def apply_alias_to_channel(channel: ET.Element, old_id: str, new_id: str) -> ET.Element:
    """应用别名映射到频道元素"""
    new_channel = ET.Element('channel', id=new_id)
    
    for child in channel:
        if MODIFY_DISPLAY_NAME and child.tag == 'display-name' and child.text:
            new_child = ET.Element(child.tag)
            new_child.text = new_id
            new_child.tail = child.tail
            for key, value in child.attrib.items():
                new_child.set(key, value)
            new_channel.append(new_child)
        else:
            new_child = copy.deepcopy(child)
            new_channel.append(new_child)
    
    new_channel.text = channel.text
    new_channel.tail = channel.tail
    
    for key, value in channel.attrib.items():
        if key != 'id':
            new_channel.set(key, value)
    
    return new_channel


def apply_alias_to_programme(programme: ET.Element, new_channel_id: str) -> ET.Element:
    """应用别名映射到节目元素"""
    new_programme = ET.Element('programme')
    
    for child in programme:
        new_programme.append(copy.deepcopy(child))
    
    new_programme.text = programme.text
    new_programme.tail = programme.tail
    
    for key, value in programme.attrib.items():
        if key == 'channel':
            new_programme.set('channel', new_channel_id)
        else:
            new_programme.set(key, value)
    
    return new_programme


# ==================== 配置解析 ====================
def parse_source(source_file: str) -> Tuple[Dict[str, Dict], Tuple[int, int]]:
    """解析EPG源配置文件"""
    try:
        with open(source_file, 'r', encoding='utf-8') as source:
            lines = source.readlines()
            
            if not lines:
                print(f'❌ 错误: 配置文件为空')
                sys.exit(1)
            
            past_days = DEFAULT_PAST_DAYS
            future_days = DEFAULT_FUTURE_DAYS
            
            for line_num, line in enumerate(lines[:5], 1):
                line = line.partition('#')[0].strip()
                if not line:
                    continue
                
                if line.upper().startswith('PAST_DAYS='):
                    try:
                        past_days = int(line.split('=', 1)[1].strip())
                        print(f'✅ 过去天数: {past_days} 天')
                    except ValueError:
                        print(f'⚠ 过去天数格式错误，使用默认值: {DEFAULT_PAST_DAYS} 天')
                
                elif line.upper().startswith('FUTURE_DAYS='):
                    try:
                        future_days = int(line.split('=', 1)[1].strip())
                        print(f'✅ 未来天数: {future_days} 天')
                    except ValueError:
                        print(f'⚠ 未来天数格式错误，使用默认值: {DEFAULT_FUTURE_DAYS} 天')
            
            total_days = past_days + future_days + 1
            print(f'✅ 总时间范围: 过去 {past_days} 天 + 当天 + 未来 {future_days} 天 = 共 {total_days} 天')
            print()
            
            print(f'📝 配置:')
            print(f'   MODIFY_CHANNEL_ID: {MODIFY_CHANNEL_ID}')
            print(f'   MODIFY_DISPLAY_NAME: {MODIFY_DISPLAY_NAME}')
            print(f'   SPLIT_OVERNIGHT_PROGRAMS: {SPLIT_OVERNIGHT_PROGRAMS}')
            print(f'   SMART_MERGE: {SMART_MERGE}')
            print(f'   BROWSER_IMPERSONATE: {BROWSER_IMPERSONATE}')
            print()
            
            data_source: Dict[str, Dict] = {}
            current_source = ''
            current_timezone = None
            current_change_tz = 'N'
            
            for line_num, line in enumerate(lines[1:], 2):
                line = line.partition('#')[0].strip()
                if not line:
                    continue
                
                if line.upper().startswith(('PAST_DAYS=', 'FUTURE_DAYS=')):
                    continue
                
                if line.startswith(('http://', 'https://')):
                    current_source = line
                    current_timezone = None
                    current_change_tz = 'N'
                    if current_source not in data_source:
                        data_source[current_source] = {
                            'timezone': None,
                            'change_timezone': 'N',
                            'channels': []
                        }
                elif current_source:
                    if line.lower().startswith('timezone='):
                        tz_str = line.split('=', 1)[1].strip()
                        current_timezone = parse_timezone(tz_str)
                        data_source[current_source]['timezone'] = current_timezone
                        if current_timezone is not None:
                            print(f'  ✅ 时区设置: {tz_str} → 将转换为北京时间')
                        else:
                            print(f'  ✅ 时区设置: {tz_str} → 北京时间，保持原样不转换')
                    
                    elif line.lower().startswith('changetimezone='):
                        change_tz_str = line.split('=', 1)[1].strip().upper()
                        if change_tz_str in ['Y', 'YES', 'TRUE']:
                            current_change_tz = 'Y'
                        else:
                            current_change_tz = 'N'
                        data_source[current_source]['change_timezone'] = current_change_tz
                        print(f'  ✅ 时区转换开关: ChangeTimezone={current_change_tz}')
                    
                    elif '\t' in line:
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            old_id = parts[0].strip()
                            new_id = parts[1].strip()
                            if old_id and new_id:
                                data_source[current_source]['channels'].append((old_id, new_id))
                                print(f'  映射: "{old_id}" → "{new_id}"')
                    else:
                        channel_id = line
                        if channel_id:
                            data_source[current_source]['channels'].append((channel_id, None))
            
            if not data_source:
                print(f'❌ 错误: 配置文件中没有找到有效的EPG源')
                sys.exit(1)
            
            return data_source, (past_days, future_days)
            
    except FileNotFoundError:
        print(f'❌ 错误: 配置文件 {source_file} 不存在！')
        sys.exit(1)
    except Exception as e:
        print(f'❌ 错误: 解析配置文件失败 - {e}')
        sys.exit(1)


def analyze_epg_time_range(program_dict: Dict[Tuple[str, str], ET.Element]) -> Tuple[int, int]:
    """分析EPG数据中实际包含的过去天数和未来天数"""
    if not program_dict:
        return 0, 0
    
    now_utc = datetime.now(UTC)
    today_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    
    min_start = None
    max_start = None
    
    for (_, start_time_str), _ in program_dict.items():
        try:
            if ' +' in start_time_str or ' -' in start_time_str:
                dt = datetime.strptime(start_time_str, '%Y%m%d%H%M%S %z')
            else:
                dt = datetime.strptime(start_time_str, '%Y%m%d%H%M%S')
                dt = dt.replace(tzinfo=BEIJING_TZ)
            
            dt_utc = dt.astimezone(UTC)
            
            if min_start is None or dt_utc < min_start:
                min_start = dt_utc
            if max_start is None or dt_utc > max_start:
                max_start = dt_utc
        except Exception:
            continue
    
    if min_start is None or max_start is None:
        return 0, 0
    
    if min_start < today_start_utc:
        past_days_actual = (today_start_utc - min_start).days + 1
    else:
        past_days_actual = 0
    
    if max_start >= today_start_utc:
        future_days_actual = (max_start - today_start_utc).days + 1
    else:
        future_days_actual = 0
    
    return past_days_actual, future_days_actual


# ==================== 文件下载（使用 curl_cffi 模拟真实浏览器）====================
def download_file(url: str, path: str) -> Optional[str]:
    """下载EPG文件（增强版）"""

    filename = os.path.basename(url.split('?')[0])

    if not filename:
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f'epg_{url_hash}.xml'

    download_path = os.path.join(path, filename)

    name, ext = os.path.splitext(filename)
    counter = 1

    while os.path.exists(download_path):
        download_path = os.path.join(
            path,
            f"{name}({counter}){ext}"
        )
        counter += 1

    print(f'    🌐 开始下载: {url}')

    for attempt in range(MAX_RETRIES + 1):

        try:
            if attempt > 0:
                wait_time = attempt * 2
                print(f'    🔄 重试 {attempt}/{MAX_RETRIES}')
                time.sleep(wait_time)

            # ==========================================
            # 第一阶段：普通 requests
            # ==========================================

            try:
                import requests

                with requests.get(
                    url,
                    timeout=20,
                    stream=True,
                    headers={
                        "User-Agent": "Mozilla/5.0"
                    }
                ) as response:

                    if response.status_code == 200:

                        content_type = response.headers.get(
                            "Content-Type", ""
                        ).lower()

                        if (
                            "xml" in content_type
                            or "gzip" in content_type
                            or "octet-stream" in content_type
                            or "text/plain" in content_type
                        ):

                            with open(download_path, 'wb') as f:
                                for chunk in response.iter_content(
                                    chunk_size=CHUNK_SIZE
                                ):
                                    if chunk:
                                        f.write(chunk)

                            # 检查文件头是否为有效 XML 或gz
                            with open(download_path, 'rb') as f_check:
                                head = f_check.read(100)

                            if (
                                head.startswith(b'<?xml')
                                or head.startswith(b'<tv')
                                or head.startswith(b'\x1f\x8b')       # gzip 文件头
                            ):
                                print(f'    ✅ requests 下载成功')
                                return download_path
                            else:
                                print(f'    ⚠ 下载内容不是有效 XML/GZIP')
                                os.remove(download_path)
                                continue

                        else:
                            print(f'    ⚠ requests 返回非XML内容: {content_type}')

            except Exception as e:
                print(f'    ⚠ requests失败: {e}')

            # ==========================================
            # 第二阶段：curl_cffi
            # ==========================================

            if HAS_CURL_CFFI:
                print(f'    🔧 尝试 curl_cffi...')
                session = curl_requests.Session()
                session.impersonate = BROWSER_IMPERSONATE
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}/"

                if 'yang-1989.eu.org' in url:
                    try:
                        session.get(base_url, timeout=10)
                        print(f'    🔥 已预热主页')
                    except Exception:
                        pass

                response = session.get(
                    url,
                    timeout=DOWNLOAD_TIMEOUT,
                    stream=True,
                    allow_redirects=True,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": base_url
                    }
                )

                if response.status_code == 200:
                    with open(download_path, 'wb') as f:
                        for chunk in response.iter_content(
                            chunk_size=CHUNK_SIZE
                        ):
                            if chunk:
                                f.write(chunk)

                    with open(download_path, 'rb') as f:
                        head = f.read(100)

                    is_valid = (
                        head.startswith(b'<?xml')
                        or head.startswith(b'<tv')
                        or head.startswith(b'\x1f\x8b')
                    )

                    if not is_valid:
                        print(f'    ❌ 下载内容不是XML/GZIP')
                        os.remove(download_path)
                        continue

                    print(f'    ✅ curl_cffi 下载成功')
                    return download_path

                else:
                    print(f'    ❌ curl_cffi HTTP错误: {response.status_code}')

        except Exception as e:
            print(f'    ❌ 下载异常: {e}')

    print(f'    ❌ 下载失败')
    return None


# ==================== EPG处理 ====================
def process_epg_source(
    file_path: str,
    source_info: Dict,
    channel_dict: Dict[str, ET.Element],
    program_dict: Dict[Tuple[str, str], ET.Element],
    start_utc: datetime,
    days_range: Tuple[int, int]
) -> None:
    """处理EPG源文件，提取频道和节目信息
    
    处理顺序：
    1. 先检测跨天节目并拆分
    2. 再按时间范围筛选
    3. 最后进行智能合并
    """
    channels_to_process = source_info['channels']
    specified_tz = source_info['timezone']
    change_timezone = source_info.get('change_timezone', 'N')
    
    past_days, future_days = days_range
    
    today_start = start_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    
    start_boundary = today_start - timedelta(days=past_days)
    end_boundary = today_start + timedelta(days=future_days + 1)
    
    print(f'    🕐 时间范围: {start_boundary.strftime("%Y-%m-%d %H:%M")} 到 {end_boundary.strftime("%Y-%m-%d %H:%M")} (UTC)')
    print(f'    📊 包含过去 {past_days} 天 + 当天 + 未来 {future_days} 天')
    
    # 处理gzip压缩文件
    if file_path.endswith('.gz'):
        dir_path = os.path.dirname(file_path)
        xml_file = os.path.join(dir_path, os.path.basename(file_path).replace('.gz', '.xml'))
        
        try:
            with gzip.open(file_path, 'rb') as gz_file:
                with open(xml_file, 'wb') as xml_file_obj:
                    xml_file_obj.write(gz_file.read())
            os.remove(file_path)
        except Exception as e:
            print(f'    ⚠ 解压失败: {e}')
            return
    else:
        xml_file = file_path
    
    # 解析XML
    try:
        tree = ET.parse(xml_file)
    except ET.ParseError:
        print(f'    ❌ XML格式错误，跳过此源')
        return
    except Exception as e:
        print(f'    ❌ 解析失败: {e}')
        return
    
    # ==================== 第一步：自动创建缺失的频道定义 ====================
    programme_channels = set()
    for programme in tree.findall('programme'):
        channel_attr = programme.attrib.get('channel', '')
        if channel_attr:
            programme_channels.add(channel_attr)
    
    existing_channel_ids = set()
    for channel in tree.findall('channel'):
        existing_channel_ids.add(channel.attrib.get('id', ''))
    
    missing_channels = programme_channels - existing_channel_ids
    
    root = tree.getroot()
    for missing_channel in missing_channels:
        temp_channel = ET.Element('channel', id=missing_channel)
        display_name = ET.SubElement(temp_channel, 'display-name')
        display_name.text = missing_channel
        root.append(temp_channel)
        print(f'    📝 自动创建频道定义: "{missing_channel}"')
    
    # ==================== 第二步：创建原ID到新ID的映射 ====================
    id_mapping = {old_id: new_id for old_id, new_id in channels_to_process if new_id}
    target_ids = {old_id for old_id, _ in channels_to_process}
    
    # ==================== 第三步：提取频道 ====================
    channels_found = 0
    for channel in tree.findall('channel'):
        original_id = channel.attrib.get('id', '')
        if original_id in target_ids:
            if MODIFY_CHANNEL_ID and original_id in id_mapping:
                final_id = id_mapping[original_id]
            else:
                final_id = original_id
            
            if final_id not in channel_dict:
                if MODIFY_CHANNEL_ID and original_id in id_mapping:
                    new_channel = apply_alias_to_channel(channel, original_id, final_id)
                    channel_dict[final_id] = new_channel
                    channels_found += 1
                else:
                    new_channel = copy.deepcopy(channel)
                    channel_dict[final_id] = new_channel
                    channels_found += 1
    
    # ==================== 第四步：显示时区处理方式 ====================
    if change_timezone == 'Y':
        print(f'    🕐 时区处理: ChangeTimezone=Y → 强制将时区改为 +0800（时间数值不变）')
    elif specified_tz is not None:
        print(f'    🕐 时区转换: 指定时区 {specified_tz} → 北京时间 (+8)')
    else:
        print(f'    🕐 时区处理: 未指定时区，保持原XML时区不变')
    
    print(f'    📝 别名映射: 修改ID={MODIFY_CHANNEL_ID}, 修改DisplayName={MODIFY_DISPLAY_NAME}')
    print(f'    ✂️ 跨天拆分: {SPLIT_OVERNIGHT_PROGRAMS}')
    print(f'    🔗 智能合并: {SMART_MERGE}')
    
    # ==================== 第五步：预处理所有节目（先拆分跨天节目）====================
    all_programmes = []
    programme_count = 0
    overnight_split_count = 0
    
    for programme in tree.findall('programme'):
        original_channel = programme.attrib.get('channel', '')
        if original_channel in target_ids:
            programme_count += 1
            
            if MODIFY_CHANNEL_ID and original_channel in id_mapping:
                final_channel = id_mapping[original_channel]
            else:
                final_channel = original_channel
            
            original_start = programme.attrib.get('start', '')
            original_stop = programme.attrib.get('stop', '')
            
            # 时间转换
            if change_timezone == 'Y':
                final_start = change_timezone_only(original_start, '+0800')
                final_stop = change_timezone_only(original_stop, '+0800')
            elif specified_tz is not None:
                source_tz = specified_tz
                final_start = convert_timezone(original_start, source_tz, BEIJING_TZ)
                final_stop = convert_timezone(original_stop, source_tz, BEIJING_TZ)
            else:
                final_start = original_start
                final_stop = original_stop
            
            # 先检查是否需要拆分跨天节目
            if SPLIT_OVERNIGHT_PROGRAMS:
                start_dt = parse_datetime_from_str(final_start)
                stop_dt = parse_datetime_from_str(final_stop)
                
                if start_dt and stop_dt and is_overnight_program(start_dt, stop_dt):
                    # 拆分跨天节目
                    split_programmes = split_overnight_program(
                        programme, start_dt, stop_dt, final_channel
                    )
                    overnight_split_count += 1
                    
                    for split_prog in split_programmes:
                        # 获取拆分后的时间
                        split_start = split_prog.attrib.get('start', '')
                        split_stop = split_prog.attrib.get('stop', '')
                        
                        # 创建 UTC 时间用于过滤
                        source_tz_from_str = extract_timezone_from_time_str(split_start)
                        filter_start = convert_date_for_filter(split_start, source_tz_from_str)
                        filter_stop = convert_date_for_filter(split_stop, source_tz_from_str)
                        
                        all_programmes.append({
                            'programme': split_prog,
                            'final_channel': final_channel,
                            'final_start': split_start,
                            'final_stop': split_stop,
                            'filter_start': filter_start,
                            'filter_stop': filter_stop,
                            'is_split': True
                        })
                    continue
            
            # 非跨天节目，直接添加
            source_tz_from_str = extract_timezone_from_time_str(final_start)
            filter_start = convert_date_for_filter(final_start, source_tz_from_str)
            filter_stop = convert_date_for_filter(final_stop, source_tz_from_str)
            
            all_programmes.append({
                'programme': programme,
                'final_channel': final_channel,
                'final_start': final_start,
                'final_stop': final_stop,
                'filter_start': filter_start,
                'filter_stop': filter_stop,
                'is_split': False
            })
    
    if overnight_split_count > 0:
        print(f'    ✂️ 跨天节目拆分: {overnight_split_count} 个')
    
    # ==================== 第六步：按时间范围筛选并添加节目 ====================
    programs_found = 0
    merged_count = 0
    total_programmes = len(all_programmes)
    
    for prog_info in all_programmes:
        filter_start = prog_info['filter_start']
        filter_stop = prog_info['filter_stop']
        final_channel = prog_info['final_channel']
        final_start = prog_info['final_start']
        final_stop = prog_info['final_stop']
        programme = prog_info['programme']
        
        if filter_start and filter_stop:
            # 检查是否在时间范围内（包含过去和未来）
            if filter_start < end_boundary and filter_stop > start_boundary:
                key = (final_channel, final_start)
                
                if SMART_MERGE and key in program_dict:
                    existing = program_dict[key]
                    new_programme = apply_alias_to_programme(programme, final_channel)
                    
                    # 更新时间和频道
                    for key_attr, value in new_programme.attrib.items():
                        if key_attr == 'start':
                            new_programme.set('start', final_start)
                        elif key_attr == 'stop':
                            new_programme.set('stop', final_stop)
                        elif key_attr == 'channel':
                            new_programme.set('channel', final_channel)
                    
                    if is_programme_more_complete(new_programme, existing):
                        program_dict[key] = new_programme
                        merged_count += 1
                elif key not in program_dict:
                    new_programme = apply_alias_to_programme(programme, final_channel)
                    
                    for key_attr, value in new_programme.attrib.items():
                        if key_attr == 'start':
                            new_programme.set('start', final_start)
                        elif key_attr == 'stop':
                            new_programme.set('stop', final_stop)
                        elif key_attr == 'channel':
                            new_programme.set('channel', final_channel)
                    
                    program_dict[key] = new_programme
                    programs_found += 1
        else:
            # 时间格式异常，仍然添加
            key = (final_channel, final_start)
            if key not in program_dict:
                new_programme = apply_alias_to_programme(programme, final_channel)
                
                for key_attr, value in new_programme.attrib.items():
                    if key_attr == 'start':
                        new_programme.set('start', final_start)
                    elif key_attr == 'stop':
                        new_programme.set('stop', final_stop)
                    elif key_attr == 'channel':
                        new_programme.set('channel', final_channel)
                
                program_dict[key] = new_programme
                programs_found += 1
    
    if merged_count > 0:
        print(f'    🔗 智能合并更新: {merged_count} 个节目')
    
    # ==================== 第七步：输出未找到的频道 ====================
    found_ids = set()
    for old_id, _ in channels_to_process:
        if MODIFY_CHANNEL_ID and old_id in id_mapping:
            final_id = id_mapping[old_id]
        else:
            final_id = old_id
        if final_id in channel_dict:
            found_ids.add(old_id)
    
    missing_channels_list = target_ids - found_ids
    if missing_channels_list:
        for channel in missing_channels_list:
            # 检查是否有映射，如果有则显示映射后的名称
            if MODIFY_CHANNEL_ID and channel in id_mapping:
                new_channel_name = id_mapping[channel]
                print(f'    ⚠ 未找到频道: {channel} → 【{new_channel_name}】')
            else:
                print(f'    ⚠ 未找到频道: {channel}')
    
    print(f'    📺 新增频道: {channels_found}/{len(target_ids)}')
    print(f'    📅 新增节目: {programs_found}/{total_programmes}')


# ==================== 主函数 ====================
def main() -> None:
    """主函数"""
    start_utc = datetime.now(UTC)
    start_beijing = start_utc.astimezone(BEIJING_TZ)
    
    print_separator('=')
    print('Guide Merger v2.0 (curl_cffi Browser Impersonation)')
    print_separator('=')
    print(f'当前时间: {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print(f'当前时间: {start_utc.strftime("%Y-%m-%d %H:%M:%S")} (UTC)')
    print()
    
    if HAS_CURL_CFFI:
        print(f'✅ 已加载 curl_cffi 库，浏览器指纹模拟: {BROWSER_IMPERSONATE}')
    else:
        print('⚠ 未安装 curl_cffi 库，将使用普通 requests')
        print('  安装方法: pip install curl-cffi')
    
    if HAS_PYPINYIN:
        print('✅ 已加载pypinyin库，支持中文拼音排序')
    else:
        print('⚠ 未安装pypinyin库')
    
    print('✅ 支持每个EPG源独立设置时区（可选，不设置则保持原时区）')
    print('✅ +8时区（北京时间）将被识别并保持原样不转换')
    print('✅ ChangeTimezone=Y 可强制将时区改为 +0800（时间数值不变）')
    print('✅ 支持前后双向时间范围（过去天数 + 当天 + 未来天数）')
    print(f'✅ 别名映射: 修改ID={MODIFY_CHANNEL_ID}, 修改DisplayName={MODIFY_DISPLAY_NAME}')
    print(f'✅ 跨天拆分: {SPLIT_OVERNIGHT_PROGRAMS}')
    print(f'✅ 智能合并: {SMART_MERGE}')
    print()
    
    print('📖 读取配置文件...')
    sources, days_range = parse_source(SOURCE_FILE)
    
    past_days, future_days = days_range
    total_days = past_days + future_days + 1
    
    print(f'✅ 找到 {len(sources)} 个EPG源')
    print(f'✅ 配置时间范围: 过去 {past_days} 天 + 当天 + 未来 {future_days} 天 = 共 {total_days} 天')
    print()
    
    for url, info in sources.items():
        print(f'  - {url}')
        if info.get('change_timezone') == 'Y':
            print(f'    时区处理: 强制转换为北京时间 (+0800)')
        elif info['timezone'] is not None:
            print(f'    时区: 指定非+8时区，将转换为北京时间')
        else:
            print(f'    时区: 保持原XML时区（可能是未指定或+8时区）')
        print(f'    频道数量: {len(info["channels"])}')
        mapping_count = sum(1 for _, new_id in info['channels'] if new_id)
        if mapping_count > 0:
            print(f'    别名映射: {mapping_count} 个')
    print()
    
    temp_dir = os.path.relpath(TEMP_DIR_NAME)
    os.makedirs(temp_dir, exist_ok=True)
    
    print('🧹 清理临时目录...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✅ 清理完成')
    print()
    
    channel_dict: Dict[str, ET.Element] = {}
    program_dict: Dict[Tuple[str, str], ET.Element] = {}
    success_count = 0
    
    for idx, (source_url, source_info) in enumerate(sources.items(), 1):
        print_separator('-')
        print(f'📡 源 {idx}/{len(sources)}: {source_url}')
        print(f'   请求频道: {len(source_info["channels"])} 个')
        
        channels_to_find = []
        for old_id, new_id in source_info['channels']:
            if MODIFY_CHANNEL_ID and new_id:
                final_id = new_id
            else:
                final_id = old_id
            if final_id not in channel_dict:
                channels_to_find.append((old_id, new_id))
        
        if not channels_to_find:
            print(f'   ⏭ 跳过: 所有频道已找到')
            print()
            continue
        
        print(f'   需要查找: {len(channels_to_find)} 个')
        
        file_path = download_file(source_url, temp_dir)
        
        if file_path:
            source_info_filtered = {
                'timezone': source_info['timezone'],
                'change_timezone': source_info.get('change_timezone', 'N'),
                'channels': channels_to_find
            }
            process_epg_source(
                file_path, source_info_filtered,
                channel_dict, program_dict,
                start_utc, days_range
            )
            success_count += 1
            print(f'   ✅ 处理成功')
        else:
            print(f'   ❌ 下载失败，跳过此源')
        
        print()
    
    if success_count == 0:
        print('❌ 错误: 所有EPG源都下载失败！')
        sys.exit(1)
    
    actual_past_days, actual_future_days = analyze_epg_time_range(program_dict)
    
    print_separator('=')
    print('📝 生成最终XML文件...')
    
    root = ET.Element('tv')
    
    comment = ET.Comment(f' Generated by Guide Merger on {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} Beijing Time ')
    root.append(comment)
    time_comment = ET.Comment(f' Time range: past {past_days} days + today + future {future_days} days ')
    root.append(time_comment)
    
    if SPLIT_OVERNIGHT_PROGRAMS:
        split_comment = ET.Comment(' Overnight programs have been split into two parts ')
        root.append(split_comment)
    
    print('🔤 应用智能排序（按display-name，数字-字母-汉字，不区分大小写）...')
    channels_sorted = sort_channels_by_display(list(channel_dict.values()))
    programmes_sorted = sort_programmes_by_display(list(program_dict.values()), channel_dict)
    
    for channel in channels_sorted:
        root.append(channel)
    for program in programmes_sorted:
        root.append(program)
    
    tree = ET.ElementTree(root)
    ET.indent(tree, space='    ', level=0)
    tree.write(OUTPUT_XML, encoding='UTF-8', xml_declaration=True)
    
    xml_size = os.path.getsize(OUTPUT_XML)
    print(f'✅ XML文件: {OUTPUT_XML}')
    print(f'  大小: {format_size(xml_size)}')
    print(f'  频道数: {len(channels_sorted)}')
    print(f'  节目数: {len(programmes_sorted)}')
    
    if channels_sorted:
        print(f'\n📺 频道排序示例（前10个）:')
        for i, channel in enumerate(channels_sorted[:10], 1):
            display_name = get_display_name(channel)
            channel_id = channel.attrib.get('id', '')
            print(f'   {i:2d}. {display_name} (ID: {channel_id})')
    
    print()
    
    print(f'🗜️ 压缩为GZIP格式...')
    if compress_gzip(OUTPUT_XML, OUTPUT_GZ):
        gz_size = os.path.getsize(OUTPUT_GZ)
        compression_ratio = (1 - gz_size / xml_size) * 100
        print(f'  ✅ 压缩率: {compression_ratio:.1f}%')
    else:
        print(f'  ⚠ GZIP压缩失败')
    
    print()
    
    print('🧹 清理临时文件...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✅ 清理完成')
    print()
    
    end_utc = datetime.now(UTC)
    end_beijing = end_utc.astimezone(BEIJING_TZ)
    duration = (end_utc - start_utc).total_seconds()
    
    print_separator('=')
    print('✅ EPG合并完成')
    print_separator('=')
    print(f'结束时间: {end_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print(f'总耗时: {duration:.2f} 秒')
    print(f'成功处理: {success_count}/{len(sources)} 个源')
    print(f'成功处理: {len(channels_sorted)} 个频道，{len(programmes_sorted)} 条节目')
    print(f'输出文件: {OUTPUT_XML} 和 {OUTPUT_GZ}')
    print(f'配置时间范围: 过去 {past_days} 天 + 当天 + 未来 {future_days} 天')
    print(f'实际时间范围: 过去 {actual_past_days} 天 + 当天 + 未来 {actual_future_days} 天')
    print_separator('=')


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('\n\n⚠ 用户中断')
        sys.exit(1)
    except Exception as e:
        print(f'\n\n❌ 程序异常: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)