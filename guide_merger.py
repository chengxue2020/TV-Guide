#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG Merger Script - 合并多个EPG源的频道节目信息
支持 .xml 和 .xml.gz 格式
支持在 source_guide.txt 中直接定义频道别名映射
支持智能排序（按display-name，数字-字母-汉字，不区分大小写）
支持每个EPG源单独设置时区转换（可选，不设置则保持原时区）
支持前后双向时间范围（包含过去和未来的节目）
可配置是否修改 channel id 和 display-name
支持保存合并前的源EPG文件到Temp文件夹（每次运行前先清空Temp文件夹）
"""

import requests
import gzip
import xml.etree.ElementTree as ET
import os
import sys
import time
import re
import shutil
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
import hashlib
import copy
from urllib.parse import urlparse

# 尝试导入Cloudflare绕过库
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    print("⚠ 未安装cloudscraper库，无法绕过Cloudflare保护")
    print("  安装方法: pip install cloudscraper")

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
SAVE_SOURCE_DIR = 'Temp'                 # 保存源EPG文件的目录
DEFAULT_TIME_FRAME = 96                  # 默认时间范围（小时）- 前后各48小时
MAX_RETRIES = 3                          # 最大重试次数
DOWNLOAD_TIMEOUT = 30                    # 下载超时（秒）
CHUNK_SIZE = 131072                      # 下载块大小（128KB）
USE_CLOUDSCRAPER = True                  # 是否使用cloudscraper绕过CF

# ==================== 别名映射配置 ====================
# True: 修改channel id
# False: 不修改channel id，保持原值
MODIFY_CHANNEL_ID = True

# True: 修改display-name
# False: 不修改display-name，保持原值
MODIFY_DISPLAY_NAME = True

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


def clean_directory(dir_path: str) -> None:
    """清空指定目录中的所有文件和子目录"""
    if not os.path.exists(dir_path):
        return
    
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        try:
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        except Exception as e:
            print(f'    ⚠ 无法删除 {item}: {e}')


def compress_gzip(input_file: str, output_file: str) -> bool:
    """压缩文件为gzip格式"""
    try:
        with open(input_file, 'rb') as f_in:
            with gzip.open(output_file, 'wb', compresslevel=9) as f_out:
                f_out.write(f_in.read())
        
        original_size = os.path.getsize(input_file)
        compressed_size = os.path.getsize(output_file)
        compression_ratio = (1 - compressed_size / original_size) * 100
        
        print(f'  ✓ 压缩完成: {format_size(compressed_size)} ({compression_ratio:.1f}% 压缩率)')
        return True
    except Exception as e:
        print(f'  ✗ 压缩失败: {e}')
        return False


def generate_source_filename(url: str) -> str:
    """
    根据URL生成源EPG文件的保存文件名
    
    规则：
    1. github.io 格式：github-账号名-源文件名.xml
    2. githubusercontent.com 格式：github-账号名-源文件名.xml
    3. 其他格式：主站名-源文件名.xml
    
    相同URL始终生成相同文件名，新文件会覆盖旧文件
    
    Args:
        url: EPG源URL
        
    Returns:
        生成的文件名
    """
    parsed = urlparse(url)
    hostname = parsed.hostname or ''
    path = parsed.path
    
    # 提取源文件名（不含扩展名）
    base_filename = os.path.basename(path)
    if base_filename.endswith('.gz'):
        base_filename = base_filename[:-3]
    if base_filename.endswith('.xml'):
        base_filename = base_filename[:-4]
    if not base_filename:
        base_filename = 'epg'
    
    # 处理 GitHub 相关域名
    if 'github.io' in hostname:
        account_name = hostname.split('.')[0]
        return f"github-{account_name}-{base_filename}.xml"
    
    elif 'githubusercontent.com' in hostname:
        path_parts = path.strip('/').split('/')
        if len(path_parts) > 0:
            account_name = path_parts[0]
            return f"github-{account_name}-{base_filename}.xml"
        else:
            return f"github-unknown-{base_filename}.xml"
    
    else:
        domain_parts = hostname.split('.')
        if len(domain_parts) >= 2:
            main_name = domain_parts[-2]
            if main_name in ['com', 'net', 'org', 'top', 'xyz', 'cn', 'cc', 'io'] and len(domain_parts) >= 3:
                main_name = domain_parts[-3]
            return f"{main_name}-{base_filename}.xml"
        else:
            return f"{hostname}-{base_filename}.xml"


def save_source_epg(content: bytes, url: str, is_gz: bool = False) -> Optional[str]:
    """
    保存合并前的源EPG文件到Temp目录
    
    Args:
        content: 文件内容（二进制）
        url: 源URL
        is_gz: 是否为gzip压缩文件
        
    Returns:
        保存的文件路径，失败返回None
    """
    os.makedirs(SAVE_SOURCE_DIR, exist_ok=True)
    
    filename = generate_source_filename(url)
    save_path = os.path.join(SAVE_SOURCE_DIR, filename)
    
    try:
        if is_gz:
            try:
                xml_content = gzip.decompress(content)
                with open(save_path, 'wb') as f:
                    f.write(xml_content)
                print(f'    📁 已保存源文件: {filename} ({format_size(len(xml_content))})')
            except Exception as e:
                print(f'    ⚠ 解压源文件失败: {e}')
                with open(save_path, 'wb') as f:
                    f.write(content)
                print(f'    📁 已保存源文件: {filename} ({format_size(len(content))})')
        else:
            with open(save_path, 'wb') as f:
                f.write(content)
            print(f'    📁 已保存源文件: {filename} ({format_size(len(content))})')
        
        return save_path
    except Exception as e:
        print(f'    ⚠ 保存源文件失败: {e}')
        return None


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
        print(f'    ✓ 检测到北京时间 (+8时区)，将保持原样不转换')
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
    """将时间字符串从源时区转换为目标时区"""
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


# ==================== 应用别名映射 ====================
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
def parse_source(source_file: str) -> Tuple[Dict[str, Dict], int]:
    """解析EPG源配置文件"""
    try:
        with open(source_file, 'r', encoding='utf-8') as source:
            lines = source.readlines()
            
            if not lines:
                print(f'✗ 错误: 配置文件为空')
                sys.exit(1)
                
            first_line = lines[0].strip()
            time_frame_string = first_line.rpartition('=')[2].strip()
            
            try:
                total_hours = int(time_frame_string)
                past_hours = total_hours // 2
                future_hours = total_hours - past_hours
                print(f'✓ 时间范围: 前后共 {total_hours} 小时')
                print(f'  (过去 {past_hours} 小时 → 未来 {future_hours} 小时)')
            except ValueError:
                total_hours = DEFAULT_TIME_FRAME
                past_hours = total_hours // 2
                future_hours = total_hours - past_hours
                print(f'⚠ 未指定时间范围，使用默认值: {DEFAULT_TIME_FRAME} 小时')
                print(f'  (过去 {past_hours} 小时 → 未来 {future_hours} 小时)')
            
            print()
            
            print(f'📝 别名映射配置:')
            print(f'   MODIFY_CHANNEL_ID: {MODIFY_CHANNEL_ID}')
            print(f'   MODIFY_DISPLAY_NAME: {MODIFY_DISPLAY_NAME}')
            print()
            
            data_source: Dict[str, Dict] = {}
            current_source = ''
            current_timezone = None
            
            for line_num, line in enumerate(lines[1:], 2):
                line = line.partition('#')[0].strip()
                if not line:
                    continue
                
                if line.startswith(('http://', 'https://')):
                    current_source = line
                    current_timezone = None
                    if current_source not in data_source:
                        data_source[current_source] = {
                            'timezone': None,
                            'channels': []
                        }
                elif current_source:
                    if line.lower().startswith('timezone='):
                        tz_str = line.split('=', 1)[1].strip()
                        current_timezone = parse_timezone(tz_str)
                        data_source[current_source]['timezone'] = current_timezone
                        if current_timezone is not None:
                            print(f'  ✓ 时区设置: {tz_str} → 将转换为北京时间')
                        else:
                            print(f'  ✓ 时区设置: {tz_str} → 北京时间，保持原样不转换')
                    
                    elif '\t' in line:
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            old_id = parts[0].strip()
                            new_id = parts[1].strip()
                            if old_id and new_id:
                                data_source[current_source]['channels'].append((old_id, new_id))
                                print(f'  ✓ 映射: "{old_id}" → "{new_id}"')
                    else:
                        channel_id = line
                        if channel_id:
                            data_source[current_source]['channels'].append((channel_id, None))
            
            if not data_source:
                print(f'✗ 错误: 配置文件中没有找到有效的EPG源')
                sys.exit(1)
            
            return data_source, total_hours
            
    except FileNotFoundError:
        print(f'✗ 错误: 配置文件 {source_file} 不存在！')
        sys.exit(1)
    except Exception as e:
        print(f'✗ 错误: 解析配置文件失败 - {e}')
        sys.exit(1)


# ==================== 文件下载 ====================
def download_file(url: str, path: str, save_source: bool = True) -> Optional[str]:
    """下载EPG文件，支持HTTP/HTTPS和Cloudflare绕过"""
    filename = os.path.basename(url.split('?')[0])
    if not filename:
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f'epg_{url_hash}.xml'
    
    download_path = os.path.join(path, filename)
    name, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(download_path):
        download_path = os.path.join(path, f"{name}({counter}){ext}")
        counter += 1
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    if '112114' in url:
        headers['Referer'] = 'https://epg.112114.xyz/'
    elif '51zjy' in url:
        headers['Referer'] = 'https://epg.51zjy.top/'
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                wait_time = attempt * 3
                print(f'    ⏳ 第 {attempt} 次重试，等待 {wait_time} 秒...')
                time.sleep(wait_time)
            
            if USE_CLOUDSCRAPER and HAS_CLOUDSCRAPER:
                scraper = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'mobile': False
                    }
                )
                response = scraper.get(url, headers=headers, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            else:
                response = requests.get(url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            
            if response.status_code == 200:
                content = response.content
                
                if save_source:
                    is_gz = url.endswith('.gz')
                    save_source_epg(content, url, is_gz)
                
                with open(download_path, 'wb') as f:
                    f.write(content)
                
                print(f'    ✓ 下载成功: {format_size(len(content))}')
                return download_path
                
            elif response.status_code == 403:
                print(f'    ✗ 访问被拒绝 (403)')
                if attempt == MAX_RETRIES:
                    return None
            elif response.status_code == 404:
                print(f'    ✗ 文件不存在 (404)')
                return None
            else:
                print(f'    ✗ HTTP错误: {response.status_code}')
                if attempt == MAX_RETRIES:
                    return None
                    
        except Exception as e:
            print(f'    ✗ 错误: {e}')
            if attempt == MAX_RETRIES:
                return None
    
    return None


# ==================== EPG处理 ====================
def process_epg_source(
    file_path: str,
    source_info: Dict,
    channel_dict: Dict[str, ET.Element],
    program_dict: Dict[Tuple[str, str], ET.Element],
    start_utc: datetime,
    total_hours: int
) -> None:
    """处理EPG源文件，提取频道和节目信息"""
    channels_to_process = source_info['channels']
    specified_tz = source_info['timezone']
    
    past_hours = total_hours // 2
    future_hours = total_hours - past_hours
    
    start_boundary = start_utc - timedelta(hours=past_hours)
    end_boundary = start_utc + timedelta(hours=future_hours)
    
    print(f'    🕐 时间范围: {start_boundary.strftime("%Y-%m-%d %H:%M")} 到 {end_boundary.strftime("%Y-%m-%d %H:%M")} (UTC)')
    print(f'    📊 包含过去 {past_hours} 小时 + 未来 {future_hours} 小时')
    
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
    
    try:
        tree = ET.parse(xml_file)
    except ET.ParseError:
        print(f'    ✗ XML格式错误')
        return
    except Exception as e:
        print(f'    ✗ 解析失败: {e}')
        return
    
    id_mapping = {old_id: new_id for old_id, new_id in channels_to_process if new_id}
    target_ids = {old_id for old_id, _ in channels_to_process}
    
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
                    if original_id != final_id:
                        print(f'    📝 频道重命名: "{original_id}" → "{final_id}"')
                else:
                    new_channel = copy.deepcopy(channel)
                    channel_dict[final_id] = new_channel
                    channels_found += 1
    
    if specified_tz is not None:
        print(f'    🕐 时区转换: 指定时区 {specified_tz} → 北京时间 (+8)')
    else:
        print(f'    🕐 时区处理: 未指定时区，保持原XML时区不变')
    
    print(f'    📝 别名映射: 修改ID={MODIFY_CHANNEL_ID}, 修改DisplayName={MODIFY_DISPLAY_NAME}')
    
    programs_found = 0
    programs_total = 0
    
    for programme in tree.findall('programme'):
        original_channel = programme.attrib.get('channel', '')
        if original_channel in target_ids:
            programs_total += 1
            
            if MODIFY_CHANNEL_ID and original_channel in id_mapping:
                final_channel = id_mapping[original_channel]
            else:
                final_channel = original_channel
            
            original_start = programme.attrib.get('start', '')
            original_stop = programme.attrib.get('stop', '')
            
            if specified_tz is not None:
                source_tz = specified_tz
                final_start = convert_timezone(original_start, source_tz, BEIJING_TZ)
                final_stop = convert_timezone(original_stop, source_tz, BEIJING_TZ)
                filter_start = convert_date_for_filter(original_start, source_tz)
                filter_stop = convert_date_for_filter(original_stop, source_tz)
            else:
                final_start = original_start
                final_stop = original_stop
                source_tz_from_str = extract_timezone_from_time_str(original_start)
                filter_start = convert_date_for_filter(original_start, source_tz_from_str)
                filter_stop = convert_date_for_filter(original_stop, source_tz_from_str)
            
            if filter_start and filter_stop:
                if filter_start < end_boundary and filter_stop > start_boundary:
                    key = (final_channel, final_start)
                    if key not in program_dict:
                        new_programme = apply_alias_to_programme(programme, final_channel)
                        
                        for key_attr, value in new_programme.attrib.items():
                            if key_attr == 'start':
                                new_programme.set('start', final_start)
                            elif key_attr == 'stop':
                                new_programme.set('stop', final_stop)
                        
                        program_dict[key] = new_programme
                        programs_found += 1
            else:
                key = (final_channel, final_start)
                if key not in program_dict:
                    new_programme = apply_alias_to_programme(programme, final_channel)
                    
                    for key_attr, value in new_programme.attrib.items():
                        if key_attr == 'start':
                            new_programme.set('start', final_start)
                        elif key_attr == 'stop':
                            new_programme.set('stop', final_stop)
                    
                    program_dict[key] = new_programme
                    programs_found += 1
    
    found_ids = set()
    for old_id, _ in channels_to_process:
        if MODIFY_CHANNEL_ID and old_id in id_mapping:
            final_id = id_mapping[old_id]
        else:
            final_id = old_id
        if final_id in channel_dict:
            found_ids.add(old_id)
    
    missing_channels = target_ids - found_ids
    if missing_channels:
        for channel in missing_channels:
            print(f'    ⚠ 未找到频道: {channel}')
    
    print(f'    📺 新增频道: {channels_found}/{len(target_ids)}')
    print(f'    📅 新增节目: {programs_found}/{programs_total}')


# ==================== 主函数 ====================
def main() -> None:
    """主函数"""
    start_utc = datetime.now(UTC)
    start_beijing = start_utc.astimezone(BEIJING_TZ)
    
    print_separator('=')
    print('Guide Merger v2.0 (Flexible Timezone & Alias Support)')
    print_separator('=')
    print(f'当前时间: {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print(f'当前时间: {start_utc.strftime("%Y-%m-%d %H:%M:%S")} (UTC)')
    print()
    
    if HAS_CLOUDSCRAPER:
        print('✓ 已加载cloudscraper库，支持绕过Cloudflare保护')
    else:
        print('⚠ 未安装cloudscraper库')
    
    if HAS_PYPINYIN:
        print('✓ 已加载pypinyin库，支持中文拼音排序')
    else:
        print('⚠ 未安装pypinyin库')
    
    print('✓ 支持每个EPG源独立设置时区（可选，不设置则保持原时区）')
    print('✓ +8时区（北京时间）将被识别并保持原样不转换')
    print('✓ 支持前后双向时间范围（包含过去和未来的节目）')
    print(f'✓ 别名映射: 修改ID={MODIFY_CHANNEL_ID}, 修改DisplayName={MODIFY_DISPLAY_NAME}')
    print(f'✓ 源文件保存: 合并前的EPG文件将保存到 {SAVE_SOURCE_DIR} 目录（每次运行前清空）')
    print()
    
    print('📖 读取配置文件...')
    sources, total_hours = parse_source(SOURCE_FILE)
    
    past_hours = total_hours // 2
    future_hours = total_hours - past_hours
    
    print(f'✓ 找到 {len(sources)} 个EPG源')
    print(f'✓ 总时间范围: {total_hours} 小时')
    print(f'  (过去 {past_hours} 小时 → 未来 {future_hours} 小时)')
    print()
    
    for url, info in sources.items():
        print(f'  - {url}')
        if info['timezone'] is not None:
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
    
    # 清空 Temp 目录（保存源EPG文件的目录）
    print(f'🧹 清空源文件目录 {SAVE_SOURCE_DIR}...')
    clean_directory(SAVE_SOURCE_DIR)
    print('✓ 清空完成')
    print()
    
    # 清理临时目录
    print(f'🧹 清理临时目录 {temp_dir}...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✓ 清理完成')
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
        
        file_path = download_file(source_url, temp_dir, save_source=True)
        
        if file_path:
            source_info_filtered = {
                'timezone': source_info['timezone'],
                'channels': channels_to_find
            }
            process_epg_source(
                file_path, source_info_filtered,
                channel_dict, program_dict,
                start_utc, total_hours
            )
            success_count += 1
            print(f'   ✓ 处理成功')
        else:
            print(f'   ✗ 下载失败，跳过此源')
        
        print()
    
    if success_count == 0:
        print('✗ 错误: 所有EPG源都下载失败！')
        sys.exit(1)
    
    print_separator('=')
    print('📝 生成最终XML文件...')
    
    root = ET.Element('tv')
    
    comment = ET.Comment(f' Generated by Guide Merger on {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} Beijing Time ')
    root.append(comment)
    time_comment = ET.Comment(f' Time range: past {past_hours}h + future {future_hours}h (total {total_hours}h) ')
    root.append(time_comment)
    
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
    print(f'✓ XML文件: {OUTPUT_XML}')
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
        print(f'  ✓ 压缩率: {compression_ratio:.1f}%')
    else:
        print(f'  ⚠ GZIP压缩失败')
    
    print()
    
    print(f'🧹 清理临时文件 {temp_dir}...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✓ 清理完成')
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
    print(f'时间范围: 过去 {past_hours} 小时 + 未来 {future_hours} 小时')
    print(f'源文件目录: {SAVE_SOURCE_DIR}')
    print_separator('=')


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('\n\n⚠ 用户中断')
        sys.exit(1)
    except Exception as e:
        print(f'\n\n✗ 程序异常: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)