#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG Merger Script - 合并多个EPG源的频道节目信息
支持 .xml 和 .xml.gz 格式
支持在 source_epg.txt 中直接定义频道别名映射
支持智能排序（按display-name，数字-字母-汉字，不区分大小写）
支持绕过Cloudflare保护
"""

import requests
import gzip
import xml.etree.ElementTree as ET
import os
import sys
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
import hashlib

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
SOURCE_FILE = 'source_epg.txt'          # EPG源配置文件
OUTPUT_XML = 'epg.xml'                   # 输出XML文件名
OUTPUT_GZ = 'epg.xml.gz'                 # 输出GZ压缩文件名
TEMP_DIR_NAME = 'temp_epg_files'         # 临时文件目录
DEFAULT_TIME_FRAME = 48                  # 默认时间范围（小时）
MAX_RETRIES = 2                          # 最大重试次数
DOWNLOAD_TIMEOUT = 30                    # 下载超时（秒）
CHUNK_SIZE = 131072                      # 下载块大小（128KB）
USE_CLOUDSCRAPER = True                  # 是否使用cloudscraper绕过CF

# ==================== 时区配置 ====================
BEIJING_TZ = timezone(timedelta(hours=8))  # 北京时区 UTC+8
UTC = timezone.utc                         # UTC时区


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
        
        print(f'  ✓ 压缩完成: {format_size(compressed_size)} ({compression_ratio:.1f}% 压缩率)')
        return True
    except Exception as e:
        print(f'  ✗ 压缩失败: {e}')
        return False


# ==================== 智能排序函数（按display-name）====================
def get_display_name(channel: ET.Element) -> str:
    """
    获取频道的显示名称
    
    优先使用 <display-name> 标签的内容，如果没有则使用 channel id
    """
    display_name = channel.find('display-name')
    if display_name is not None and display_name.text:
        return display_name.text.strip()
    # 如果没有 display-name，则使用 channel id
    return channel.attrib.get('id', '')


def get_sort_key_by_display(channel_name: str) -> Tuple[int, str, str]:
    """
    生成排序键，实现：数字 → 字母 → 汉字 的排序
    
    排序规则：
    1. 类型优先级：数字(0) < 字母(1) < 汉字(2)
    2. 同类型内：
       - 数字按数值排序
       - 字母不区分大小写排序
       - 汉字按拼音排序（如果安装了pypinyin）
    
    Args:
        channel_name: 频道显示名称
        
    Returns:
        排序用的元组 (类型优先级, 排序字符串, 原始字符串)
    """
    if not channel_name:
        return (3, '', '')
    
    # 判断第一个字符的类型
    first_char = channel_name[0]
    
    # 数字
    if first_char.isdigit():
        # 提取开头的数字
        match = re.match(r'^(\d+)', channel_name)
        if match:
            num = int(match.group(1))
            # 数字部分用数值排序，剩余部分作为辅助排序（转小写）
            remaining = channel_name[len(match.group(1)):].lower()
            return (0, f"{num:010d}", remaining)
        return (0, channel_name.lower(), channel_name)
    
    # 英文字母（包括大小写）
    elif first_char.isalpha() and first_char.isascii():
        # 统一转换为小写进行排序（不区分大小写）
        return (1, channel_name.lower(), channel_name)
    
    # 汉字
    elif '\u4e00' <= first_char <= '\u9fff':
        if HAS_PYPINYIN:
            # 使用拼音排序（拼音不区分大小写）
            try:
                pinyin_list = pinyin(channel_name, style=Style.NORMAL)
                pinyin_str = ''.join([p[0].lower() for p in pinyin_list])
                return (2, pinyin_str, channel_name)
            except:
                # 拼音转换失败，使用原始字符串
                return (2, channel_name, channel_name)
        else:
            # 没有拼音库，使用原始字符串
            return (2, channel_name, channel_name)
    
    # 其他字符（符号等）
    else:
        return (3, channel_name.lower(), channel_name)


def sort_channels_by_display(channels: List[ET.Element]) -> List[ET.Element]:
    """
    智能排序频道列表（按display-name）
    
    排序规则：数字 → 字母 → 汉字
    - 数字：按数值大小排序（1, 2, 10, 100...）
    - 字母：按字母顺序排序（不区分大小写，A和a视为相同）
    - 汉字：按拼音首字母排序（需要pypinyin库）
    """
    def channel_key(channel):
        display_name = get_display_name(channel)
        return get_sort_key_by_display(display_name)
    
    return sorted(channels, key=channel_key)


def sort_programmes_by_display(programmes: List[ET.Element], 
                                channel_dict: Dict[str, ET.Element]) -> List[ET.Element]:
    """
    智能排序节目列表（按频道的display-name排序）
    
    先按频道的display-name排序，再按开始时间排序
    """
    # 创建频道ID到display-name的映射
    channel_display_map = {}
    for channel_id, channel in channel_dict.items():
        channel_display_map[channel_id] = get_display_name(channel)
    
    def programme_key(programme):
        channel_id = programme.attrib.get('channel', '')
        display_name = channel_display_map.get(channel_id, channel_id)
        start_time = programme.attrib.get('start', '')
        # 使用display-name的排序键和开始时间
        return (get_sort_key_by_display(display_name), start_time)
    
    return sorted(programmes, key=programme_key)


# ==================== 配置解析 ====================
def parse_source(source_file: str) -> Tuple[Dict[str, List[Tuple[str, Optional[str]]]], int]:
    """
    解析EPG源配置文件，支持频道别名映射
    
    文件格式示例：
    timeframe=96
    
    https://epg.iill.top/epg.xml.gz
    1	CCTV1          # 带别名映射，将源中的"1"映射为"CCTV1"
    2	CCTV2          # 带别名映射，将源中的"2"映射为"CCTV2"
    明珠台            # 不带映射，直接使用"明珠台"
    BBC Earth
    BBC Lifestyle
    
    http://e.erw.cc/e.xml.gz
    1                # 不带映射，直接使用"1"
    2
    3
    
    Returns:
        (数据源字典, 时间范围)
        数据源字典格式: {URL: [(原频道ID, 新频道ID或None), ...]}
    """
    try:
        with open(source_file, 'r', encoding='utf-8') as source:
            lines = source.readlines()
            
            # 解析第一行获取时间范围
            if not lines:
                print(f'✗ 错误: 配置文件为空')
                sys.exit(1)
                
            first_line = lines[0].strip()
            time_frame_string = first_line.rpartition('=')[2].strip()
            
            try:
                time_frame = int(time_frame_string)
                print(f'✓ 时间范围: {time_frame} 小时')
            except ValueError:
                time_frame = DEFAULT_TIME_FRAME
                print(f'⚠ 未指定时间范围，使用默认值: {DEFAULT_TIME_FRAME} 小时')
            
            print()
            
            # 解析源和频道
            data_source: Dict[str, List[Tuple[str, Optional[str]]]] = {}
            current_source = ''
            
            for line_num, line in enumerate(lines[1:], 2):
                # 移除注释和空白
                line = line.partition('#')[0].strip()
                if not line:
                    continue
                
                # 判断是URL还是频道ID
                if line.startswith(('http://', 'https://')):
                    current_source = line
                    if current_source not in data_source:
                        data_source[current_source] = []
                elif current_source:
                    # 检查是否包含Tab键（别名映射）
                    if '\t' in line:
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            old_id = parts[0].strip()
                            new_id = parts[1].strip()
                            if old_id and new_id:
                                data_source[current_source].append((old_id, new_id))
                                print(f'  ✓ 映射: "{old_id}" → "{new_id}"')
                            else:
                                print(f'  ⚠ 第{line_num}行格式错误: {line}')
                        else:
                            print(f'  ⚠ 第{line_num}行格式错误: {line}')
                    else:
                        # 直接使用频道ID（无映射）
                        channel_id = line
                        if channel_id:
                            data_source[current_source].append((channel_id, None))
            
            # 验证是否有数据
            if not data_source:
                print(f'✗ 错误: 配置文件中没有找到有效的EPG源')
                sys.exit(1)
            
            return data_source, time_frame
            
    except FileNotFoundError:
        print(f'✗ 错误: 配置文件 {source_file} 不存在！')
        sys.exit(1)
    except Exception as e:
        print(f'✗ 错误: 解析配置文件失败 - {e}')
        sys.exit(1)


# ==================== 文件下载（支持Cloudflare绕过）====================
def download_file(url: str, path: str) -> Optional[str]:
    """
    下载EPG文件，支持HTTP/HTTPS和Cloudflare绕过
    
    Args:
        url: 下载URL
        path: 保存路径
        
    Returns:
        成功返回文件路径，失败返回None
    """
    # 提取文件名
    filename = os.path.basename(url.split('?')[0])
    if not filename:
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f'epg_{url_hash}.xml'
    
    # 处理文件名冲突
    download_path = os.path.join(path, filename)
    name, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(download_path):
        download_path = os.path.join(path, f"{name}({counter}){ext}")
        counter += 1
    
    # 设置请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    # 为特定域名添加Referer
    if '112114' in url:
        headers['Referer'] = 'https://epg.112114.xyz/'
    elif '51zjy' in url:
        headers['Referer'] = 'https://epg.51zjy.top/'
    
    # 重试下载
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                wait_time = attempt * 3
                print(f'    ⏳ 第 {attempt} 次重试，等待 {wait_time} 秒...')
                time.sleep(wait_time)
            
            # 选择下载方式
            if USE_CLOUDSCRAPER and HAS_CLOUDSCRAPER:
                # 使用cloudscraper绕过Cloudflare
                scraper = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'mobile': False
                    }
                )
                response = scraper.get(url, headers=headers, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            else:
                # 使用普通requests
                response = requests.get(url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True)
            
            # 检查响应状态
            if response.status_code == 200:
                # 检查是否是Cloudflare挑战页面
                content_preview = response.text[:500] if hasattr(response, 'text') else ''
                if 'cf-challenge' in content_preview or 'cloudflare' in content_preview.lower():
                    print(f'    ⚠ 检测到Cloudflare挑战页面，尝试继续...')
                
                # 写入文件
                with open(download_path, 'wb') as f:
                    downloaded = 0
                    if USE_CLOUDSCRAPER and HAS_CLOUDSCRAPER:
                        # cloudscraper返回的是content，不是stream
                        f.write(response.content)
                        downloaded = len(response.content)
                    else:
                        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                
                print(f'    ✓ 下载成功: {format_size(downloaded)}')
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
            error_msg = str(e)
            if 'cloudflare' in error_msg.lower() or 'challenge' in error_msg.lower():
                print(f'    ✗ Cloudflare保护: {e}')
                if attempt == MAX_RETRIES:
                    print(f'    ⚠ 提示: 该网站可能启用了Cloudflare保护，可以尝试安装cloudscraper库')
                    return None
            else:
                print(f'    ✗ 错误: {e}')
                if attempt == MAX_RETRIES:
                    return None
    
    return None


# ==================== 日期转换 ====================
def convert_date(epg_format_date: str) -> Optional[datetime]:
    """转换EPG日期字符串为datetime对象（统一返回UTC时间）"""
    if not epg_format_date:
        return None
    
    try:
        date_obj = datetime.strptime(epg_format_date, '%Y%m%d%H%M%S %z')
        return date_obj.astimezone(UTC)
    except ValueError:
        try:
            date_obj = datetime.strptime(epg_format_date, '%Y%m%d%H%M%S')
            return date_obj.replace(tzinfo=UTC)
        except Exception:
            return None


# ==================== EPG处理 ====================
def process_epg_source(
    file_path: str,
    channels_to_process: List[Tuple[str, Optional[str]]],
    channel_dict: Dict[str, ET.Element],
    program_dict: Dict[Tuple[str, str], ET.Element],
    start_utc: datetime,
    time_frame: int
) -> None:
    """
    处理EPG源文件，提取频道和节目信息
    
    Args:
        file_path: EPG文件路径
        channels_to_process: 需要处理的频道列表，每个元素为 (原ID, 新ID或None)
        channel_dict: 频道字典
        program_dict: 节目字典
        start_utc: 起始时间（UTC）
        time_frame: 时间范围（小时）
    """
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
        print(f'    ✗ XML格式错误')
        return
    except Exception as e:
        print(f'    ✗ 解析失败: {e}')
        return
    
    # 创建原ID到新ID的映射
    id_mapping = {old_id: new_id for old_id, new_id in channels_to_process if new_id}
    target_ids = {old_id for old_id, _ in channels_to_process}
    
    # 提取频道（去重并应用别名）
    channels_found = 0
    for channel in tree.findall('channel'):
        original_id = channel.attrib.get('id', '')
        if original_id in target_ids:
            # 确定最终使用的频道ID
            final_id = id_mapping.get(original_id, original_id)
            
            # 如果最终ID不在字典中，添加
            if final_id not in channel_dict:
                # 创建频道的副本并修改ID
                new_channel = ET.Element('channel', id=final_id)
                # 复制所有子元素
                for child in channel:
                    new_channel.append(child)
                # 复制文本和属性
                new_channel.text = channel.text
                new_channel.tail = channel.tail
                for key, value in channel.attrib.items():
                    if key != 'id':
                        new_channel.set(key, value)
                
                channel_dict[final_id] = new_channel
                channels_found += 1
                if original_id != final_id:
                    print(f'    📝 频道重命名: "{original_id}" → "{final_id}"')
    
    # 提取节目（去重并应用别名）
    programs_found = 0
    programs_total = 0
    
    for programme in tree.findall('programme'):
        original_channel = programme.attrib.get('channel', '')
        if original_channel in target_ids:
            programs_total += 1
            
            # 确定最终使用的频道ID
            final_channel = id_mapping.get(original_channel, original_channel)
            
            program_start = convert_date(programme.attrib.get('start', ''))
            program_stop = convert_date(programme.attrib.get('stop', ''))
            
            if program_start and program_stop:
                start_delta = (program_start - start_utc).total_seconds() / 3600
                stop_delta = (program_stop - start_utc).total_seconds() / 3600
                
                if start_delta < time_frame and stop_delta > 0:
                    # 使用最终频道ID和开始时间作为唯一键
                    key = (final_channel, programme.attrib.get('start', ''))
                    if key not in program_dict:
                        # 创建节目的副本并修改channel属性
                        new_programme = ET.Element('programme')
                        for child in programme:
                            new_programme.append(child)
                        new_programme.text = programme.text
                        new_programme.tail = programme.tail
                        # 复制所有属性，但修改channel
                        for key_attr, value in programme.attrib.items():
                            if key_attr == 'channel':
                                new_programme.set('channel', final_channel)
                            else:
                                new_programme.set(key_attr, value)
                        
                        program_dict[key] = new_programme
                        programs_found += 1
            else:
                # 时间格式异常，仍然添加
                key = (final_channel, programme.attrib.get('start', ''))
                if key not in program_dict:
                    new_programme = ET.Element('programme')
                    for child in programme:
                        new_programme.append(child)
                    new_programme.text = programme.text
                    new_programme.tail = programme.tail
                    for key_attr, value in programme.attrib.items():
                        if key_attr == 'channel':
                            new_programme.set('channel', final_channel)
                        else:
                            new_programme.set(key_attr, value)
                    
                    program_dict[key] = new_programme
                    programs_found += 1
    
    # 输出统计
    found_ids = set()
    for old_id, _ in channels_to_process:
        final_id = id_mapping.get(old_id, old_id)
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
    print('EPG Merger v2.0 (with Cloudflare Bypass & Display Name Sort)')
    print_separator('=')
    print(f'开始时间: {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print()
    
    # 显示库状态
    if HAS_CLOUDSCRAPER:
        print('✓ 已加载cloudscraper库，支持绕过Cloudflare保护')
    else:
        print('⚠ 未安装cloudscraper库，无法绕过Cloudflare保护')
        print('  安装方法: pip install cloudscraper')
    
    if HAS_PYPINYIN:
        print('✓ 已加载pypinyin库，支持中文拼音排序')
    else:
        print('⚠ 未安装pypinyin库，中文将按Unicode排序')
        print('  安装方法: pip install pypinyin')
    print()
    
    # 解析配置
    print('📖 读取配置文件...')
    sources, time_frame = parse_source(SOURCE_FILE)
    
    print(f'✓ 找到 {len(sources)} 个EPG源')
    print(f'✓ 时间范围: {time_frame} 小时')
    print()
    
    # 打印源信息
    for url, channels in sources.items():
        print(f'  - {url}')
        print(f'    频道数量: {len(channels)}')
        mapping_count = sum(1 for _, new_id in channels if new_id)
        if mapping_count > 0:
            print(f'    别名映射: {mapping_count} 个')
    print()
    
    # 准备临时目录
    temp_dir = os.path.relpath(TEMP_DIR_NAME)
    os.makedirs(temp_dir, exist_ok=True)
    
    # 清理临时目录
    print('🧹 清理临时目录...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✓ 清理完成')
    print()
    
    # 处理EPG源
    channel_dict: Dict[str, ET.Element] = {}
    program_dict: Dict[Tuple[str, str], ET.Element] = {}
    success_count = 0
    
    for idx, (source_url, channel_list) in enumerate(sources.items(), 1):
        print_separator('-')
        print(f'📡 源 {idx}/{len(sources)}: {source_url}')
        print(f'   请求频道: {len(channel_list)} 个')
        
        # 过滤已找到的频道
        channels_to_find = []
        for old_id, new_id in channel_list:
            final_id = new_id if new_id else old_id
            if final_id not in channel_dict:
                channels_to_find.append((old_id, new_id))
        
        if not channels_to_find:
            print(f'   ⏭ 跳过: 所有频道已找到')
            print()
            continue
        
        print(f'   需要查找: {len(channels_to_find)} 个')
        
        # 下载文件
        file_path = download_file(source_url, temp_dir)
        
        # 处理文件
        if file_path:
            process_epg_source(
                file_path, channels_to_find,
                channel_dict, program_dict,
                start_utc, time_frame
            )
            success_count += 1
            print(f'   ✓ 处理成功')
        else:
            print(f'   ✗ 下载失败，跳过此源')
        
        print()
    
    # 检查是否有成功处理的源
    if success_count == 0:
        print('✗ 错误: 所有EPG源都下载失败！')
        print('提示: 如果源启用了Cloudflare保护，请安装cloudscraper库: pip install cloudscraper')
        sys.exit(1)
    
    # 生成最终XML
    print_separator('=')
    print('📝 生成最终XML文件...')
    
    root = ET.Element('tv')
    
    # 添加生成信息
    comment = ET.Comment(f' Generated by EPG Merger on {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} Beijing Time ')
    root.append(comment)
    
    # 使用智能排序（按display-name，不区分大小写）
    print('🔤 应用智能排序（按display-name，数字-字母-汉字，不区分大小写）...')
    channels_sorted = sort_channels_by_display(list(channel_dict.values()))
    programmes_sorted = sort_programmes_by_display(list(program_dict.values()), channel_dict)
    
    for channel in channels_sorted:
        root.append(channel)
    for program in programmes_sorted:
        root.append(program)
    
    # 写入XML文件
    tree = ET.ElementTree(root)
    ET.indent(tree, space='    ', level=0)
    tree.write(OUTPUT_XML, encoding='UTF-8', xml_declaration=True)
    
    xml_size = os.path.getsize(OUTPUT_XML)
    print(f'✓ XML文件: {OUTPUT_XML}')
    print(f'  大小: {format_size(xml_size)}')
    print(f'  频道数: {len(channels_sorted)}')
    print(f'  节目数: {len(programmes_sorted)}')
    
    # 显示排序示例（前10个频道）
    if channels_sorted:
        print(f'\n📺 频道排序示例（前10个）:')
        for i, channel in enumerate(channels_sorted[:10], 1):
            display_name = get_display_name(channel)
            channel_id = channel.attrib.get('id', '')
            print(f'   {i:2d}. {display_name} (ID: {channel_id})')
    
    print()
    
    # 压缩为GZIP文件
    print(f'🗜️ 压缩为GZIP格式...')
    if compress_gzip(OUTPUT_XML, OUTPUT_GZ):
        gz_size = os.path.getsize(OUTPUT_GZ)
        compression_ratio = (1 - gz_size / xml_size) * 100
        print(f'  ✓ 压缩率: {compression_ratio:.1f}%')
        print(f'  ✓ 原始大小: {format_size(xml_size)} → 压缩后: {format_size(gz_size)}')
    else:
        print(f'  ⚠ GZIP压缩失败，仅生成XML文件')
    
    print()
    
    # 清理临时文件
    print('🧹 清理临时文件...')
    for temp_file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, temp_file))
        except Exception:
            pass
    print('✓ 清理完成')
    print()
    
    # 结束时间
    end_utc = datetime.now(UTC)
    end_beijing = end_utc.astimezone(BEIJING_TZ)
    duration = (end_utc - start_utc).total_seconds()
    
    print_separator('=')
    print('✅ EPG合并完成')
    print_separator('=')
    print(f'结束时间: {end_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
    print(f'总耗时: {duration:.2f} 秒')
    print(f'成功处理: {success_count}/{len(sources)} 个源')
    print(f'输出文件: {OUTPUT_XML} 和 {OUTPUT_GZ}')
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