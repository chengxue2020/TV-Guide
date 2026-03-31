#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG Merger Script - 合并多个EPG源的频道节目信息
支持 .xml 和 .xml.gz 格式
"""

import requests
import gzip
import xml.etree.ElementTree as ET
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
import hashlib

# ==================== 配置常量 ====================
SOURCE_FILE = 'source_epg.txt'          # EPG源配置文件
OUTPUT_XML = 'epg.xml'                   # 输出文件名
TEMP_DIR_NAME = 'temp_epg_files'         # 临时文件目录
DEFAULT_TIME_FRAME = 48                  # 默认时间范围（小时）
MAX_RETRIES = 2                          # 最大重试次数
DOWNLOAD_TIMEOUT = 30                    # 下载超时（秒）
CHUNK_SIZE = 131072                      # 下载块大小（128KB）

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


# ==================== 配置解析 ====================
def parse_source(source_file: str) -> Tuple[Dict[str, List[str]], int]:
    """
    解析EPG源配置文件
    
    文件格式示例：
    timeframe=96
    
    https://epg.iill.top/epg.xml.gz
    明珠台
    BBC Earth
    
    https://epg.112114.xyz/pp.xml
    CCTV1
    CCTV5
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
            data_source: Dict[str, List[str]] = {}
            current_source = ''
            
            for line_num, line in enumerate(lines[1:], 2):  # 跳过第一行
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
                    channel_id = line
                    if channel_id not in data_source[current_source]:
                        data_source[current_source].append(channel_id)
            
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


# ==================== 文件下载 ====================
def download_file(url: str, path: str) -> Optional[str]:
    """下载EPG文件，支持HTTP/HTTPS和重定向"""
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
                wait_time = attempt * 2
                print(f'    ⏳ 第 {attempt} 次重试，等待 {wait_time} 秒...')
                time.sleep(wait_time)
            
            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=DOWNLOAD_TIMEOUT,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                with open(download_path, 'wb') as f:
                    downloaded = 0
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
                    
        except requests.exceptions.Timeout:
            print(f'    ✗ 连接超时')
            if attempt == MAX_RETRIES:
                return None
        except requests.exceptions.ConnectionError:
            print(f'    ✗ 连接错误')
            if attempt == MAX_RETRIES:
                return None
        except Exception as e:
            print(f'    ✗ 错误: {e}')
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
    channels_to_process: List[str],
    channel_dict: Dict[str, ET.Element],
    program_dict: Dict[Tuple[str, str], ET.Element],
    start_utc: datetime,
    time_frame: int
) -> None:
    """处理EPG源文件，提取频道和节目信息"""
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
    
    # 提取频道（去重）
    channels_found = 0
    channels_set = set(channels_to_process)
    
    for channel in tree.findall('channel'):
        channel_id = channel.attrib.get('id', '')
        if channel_id in channels_set and channel_id not in channel_dict:
            channel_dict[channel_id] = channel
            channels_found += 1
    
    # 提取节目（去重）
    programs_found = 0
    programs_total = 0
    
    for programme in tree.findall('programme'):
        channel_id = programme.attrib.get('channel', '')
        if channel_id in channels_set:
            programs_total += 1
            
            program_start = convert_date(programme.attrib.get('start', ''))
            program_stop = convert_date(programme.attrib.get('stop', ''))
            
            if program_start and program_stop:
                start_delta = (program_start - start_utc).total_seconds() / 3600
                stop_delta = (program_stop - start_utc).total_seconds() / 3600
                
                if start_delta < time_frame and stop_delta > 0:
                    key = (channel_id, programme.attrib.get('start', ''))
                    if key not in program_dict:
                        program_dict[key] = programme
                        programs_found += 1
            else:
                key = (channel_id, programme.attrib.get('start', ''))
                if key not in program_dict:
                    program_dict[key] = programme
                    programs_found += 1
    
    # 输出统计
    missing_channels = channels_set - set(channel_dict.keys())
    if missing_channels:
        for channel in missing_channels:
            print(f'    ⚠ 未找到频道: {channel}')
    
    print(f'    📺 新增频道: {channels_found}/{len(channels_set)}')
    print(f'    📅 新增节目: {programs_found}/{programs_total}')


# ==================== 主函数 ====================
def main() -> None:
    """主函数"""
    start_utc = datetime.now(UTC)
    start_beijing = start_utc.astimezone(BEIJING_TZ)
    
    print_separator('=')
    print('EPG Merger v2.0')
    print_separator('=')
    print(f'开始时间: {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} (北京时间)')
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
        channels_to_find = [ch for ch in channel_list if ch not in channel_dict]
        
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
        sys.exit(1)
    
    # 生成最终XML
    print_separator('=')
    print('📝 生成最终XML文件...')
    
    root = ET.Element('tv')
    
    # 添加生成信息
    comment = ET.Comment(f' Generated by EPG Merger on {start_beijing.strftime("%Y-%m-%d %H:%M:%S")} Beijing Time ')
    root.append(comment)
    
    # 排序频道和节目
    channels_sorted = sorted(channel_dict.values(), key=lambda c: c.attrib.get('id', '').lower())
    programs_sorted = sorted(
        program_dict.values(),
        key=lambda p: (p.attrib.get('channel', '').lower(), p.attrib.get('start', ''))
    )
    
    for channel in channels_sorted:
        root.append(channel)
    for program in programs_sorted:
        root.append(program)
    
    # 写入文件
    tree = ET.ElementTree(root)
    ET.indent(tree, space='    ', level=0)
    tree.write(OUTPUT_XML, encoding='UTF-8', xml_declaration=True)
    
    xml_size = os.path.getsize(OUTPUT_XML)
    
    print(f'✓ 输出文件: {OUTPUT_XML}')
    print(f'✓ 文件大小: {format_size(xml_size)}')
    print(f'✓ 总频道数: {len(channels_sorted)}')
    print(f'✓ 总节目数: {len(programs_sorted)}')
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
