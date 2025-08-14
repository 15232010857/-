import requests
from bs4 import BeautifulSoup
import re
import time
import random
import schedule
from datetime import datetime
import csv
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
import pymysql
from pymysql.cursors import DictCursor
import threading
from queue import Queue
import signal
from collections import defaultdict

# -------------------------- 配置项 --------------------------
DB_CONFIG = {
    "host": "110.42.39.180",
    "port": 3306,
    "user": "vicskins",
    "password": "Vicskins123@",
    "db": "vicskins",
    "charset": "utf8mb4"
}

# 最大重试次数（失败页面最多重试3次）
MAX_RETRIES = 3
# -----------------------------------------------------------

# 所有分类配置（已将所有页数修改为1以便测试）
CATEGORIES = {
    "匕首": (
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_Knife&page=1&limit=40',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_Knife&page={page}&limit=40',
        62,  # 总页数改为1
        1  # 起始页改为1
    ),
    "步枪": (
        'https://www.zbt.com/steam/csgo.html?filter=type%3DCSGO_Type_Rifle&page=1&limit=40',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_Rifle&page={page}&limit=40',
        84,
        1
    ),
    "手枪": (
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_Pistol&page=1&limit=40',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_Pistol&page={page}&limit=40',
        76,
        1
    ),
    "微冲": (
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_SMG&page=1&limit=40&orderBy=4',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_SMG&page={page}&limit=40&orderBy=4',
        52,
        1
    ),
    "重武器": (
        'https://www.zbt.com/steam/csgo.html?filter=type%3DCSGO_Type_Machinegun&page=1&limit=40&orderBy=1',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Type_Machinegun&page={page}&limit=40&orderBy=1',
        9,
        1
    ),
    "手套": (
        'https://www.zbt.com/steam/csgo.html?filter=type%3DType_Hands&page=1&limit=40&orderBy=1',
        'https://www.zbt.com/steam/csgo.html?filter=type%3DType_Hands&page={page}&limit=40&orderBy=1',
        9,
        1
    ),
    "印花-1": (
        'https://www.zbt.com/steam/csgo.html?filter=type%3DCSGO_Tool_Sticker&page=1&limit=40&orderBy=1',
        'https://www.zbt.com/steam/csgo.html?filter=type%3DCSGO_Tool_Sticker&page={page}&limit=40&orderBy=1',
        68,
        1
    ),
    "印花-2": (
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Tool_Sticker&page=69&limit=40&orderBy=1',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Tool_Sticker&page={page}&limit=40&orderBy=1',
        68,
        69  # 起始页改为1
    ),
    "印花-3": (
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Tool_Sticker&page=137&limit=40&orderBy=1',
        'https://www.zbt.com/cn/steam/csgo.html?filter=type%3DCSGO_Tool_Sticker&page={page}&limit=40&orderBy=1',
        68,
        137  # 起始页改为1
    ),
    "其他": (
        'https://www.zbt.com/steam/csgo.html?filter=type%3DCSGO_Type_other&page=1&orderBy=1&limit=40',
        'https://www.zbt.com/steam/csgo.html?filter=type%3DCSGO_Type_other&page={page}&limit=40',
        57,
        1
    )
}

# -------------------------- 核心组件：队列与状态 --------------------------
DATA_QUEUE = Queue(maxsize=10000)
BATCH_SIZE = 500
exit_event = threading.Event()

# 新增：失败页面记录（格式：{分类: {页码: 重试次数}}）
failed_pages = defaultdict(dict)
failed_pages_lock = threading.Lock()  # 线程安全锁

# 全局变量与锁
crawl_id = 1
id_lock = threading.Lock()
invalid_data_lock = threading.Lock()
category_running = {category: False for category in CATEGORIES.keys()}
running_lock = threading.Lock()
csv_first_write = defaultdict(lambda: True)
csv_lock = threading.Lock()

ua = UserAgent()

# -------------------------- 随机Cookies池 --------------------------
COOKIES_POOL = [
    {
        'zbt_game': '570',
        'zbt_lang': 'zh-CN',
        'device': 'f06991a9131741cb0a4cb55d38c14a36',
        'Hm_lvt_33339afff673f7484c8b0d7b30ea6a66': '1753705359',
        'Hm_lpvt_33339afff673f7484c8b0d7b30ea6a66': '1753705359',
        'HMACCOUNT': 'FEF012DBBC5F3190',
        'zbt_registerGuide': '1',
        '_zbt_steam_alert': '1',
    },
    {
        'zbt_game': '570',
        'zbt_lang': 'zh-CN',
        'device': 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6',
        'Hm_lvt_33339afff673f7484c8b0d7b30ea6a66': '1753712456',
        'Hm_lpvt_33339afff673f7484c8b0d7b30ea6a66': '1753712456',
        'HMACCOUNT': 'A1B2C3D4E5F6G7H8',
        'zbt_registerGuide': '1',
        '_zbt_steam_alert': '1',
    },
    {
        'zbt_game': '570',
        'zbt_lang': 'zh-CN',
        'device': 'z9y8x7w6v5u4t3s2r1q0p9o8n7m6l5',
        'Hm_lvt_33339afff673f7484c8b0d7b30ea6a66': '1753723456',
        'Hm_lpvt_33339afff673f7484c8b0d7b30ea6a66': '1753723456',
        'HMACCOUNT': 'Z9Y8X7W6V5U4T3S2',
        'zbt_registerGuide': '1',
        '_zbt_steam_alert': '0',
    },
    {
        'zbt_game': '570',
        'zbt_lang': 'zh-CN',
        'device': 'k1j2i3h4g5f6e7d8c9b0a1s2d3f4g5',
        'Hm_lvt_33339afff673f7484c8b0d7b30ea6a66': '1753734567',
        'Hm_lpvt_33339afff673f7484c8b0d7b30ea6a66': '1753734567',
        'HMACCOUNT': 'K1J2I3H4G5F6E7D8',
        'zbt_registerGuide': '0',
        '_zbt_steam_alert': '1',
    }
]


def get_random_cookies():
    return random.choice(COOKIES_POOL)


# -----------------------------------------------------------------------------------
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504, 403],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.timeout = 20
    return session


def get_headers():
    return {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': ua.random
    }


# -------------------------- 数据库操作函数 --------------------------
def get_db_connection():
    try:
        return pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            db=DB_CONFIG["db"],
            charset=DB_CONFIG["charset"],
            cursorclass=DictCursor
        )
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        return None


def get_max_id():
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(id) as max_id FROM vic_skins;")
            result = cursor.fetchone()
            return result['max_id'] if result['max_id'] is not None else 0
    except Exception as e:
        print(f"获取最大ID失败: {str(e)}")
        return 0
    finally:
        if conn:
            conn.close()


def check_data_exists_batch(hashnames):
    conn = get_db_connection()
    if not conn:
        return {}
    try:
        with conn.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(hashnames))
            cursor.execute(f"SELECT hashname, id FROM vic_skins WHERE hashname IN ({placeholders});", hashnames)
            results = cursor.fetchall()
            return {item['hashname']: item['id'] for item in results}
    except Exception as e:
        print(f"批量检查数据存在失败: {str(e)}")
        return {}
    finally:
        if conn:
            conn.close()


def batch_save_or_update(batch_data):
    if not batch_data:
        return True

    print(f"【写入线程】开始批量处理{len(batch_data)}条数据")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            hashnames = [data["商品英文名"] for data in batch_data if data.get("商品英文名")]
            exists_map = check_data_exists_batch(hashnames)

            insert_data = []
            update_data = []
            for data in batch_data:
                hashname = data["商品英文名"]
                if hashname in exists_map:
                    update_data.append((data, exists_map[hashname]))
                else:
                    insert_data.append(data)

            insert_ids = []
            with id_lock:
                global crawl_id
                start_id = crawl_id
                insert_ids = list(range(start_id, start_id + len(insert_data)))
                crawl_id = start_id + len(insert_data)

            conn = get_db_connection()
            if not conn:
                raise Exception("数据库连接失败")

            with conn.cursor() as cursor:
                if insert_data:
                    insert_sql = """
                                 INSERT INTO vic_skins (id, name, hashname, sell_min, sell_num,
                                                        buy_max, buy_num, `group`, exterior, rarity,
                                                        time_create, time_update)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                 """
                    insert_params = []
                    for idx, data in enumerate(insert_data):
                        sell_min_str = data["人民币价格"].replace("¥", "").replace(",", "")
                        sell_min = float(sell_min_str) if sell_min_str and sell_min_str != "未获取到" else None
                        sell_num_str = re.sub(r'\D', '', data["在售数量"])
                        sell_num = int(sell_num_str) if sell_num_str else None
                        current_time = data["爬取时间"]

                        insert_params.append((
                            insert_ids[idx],
                            data["商品名称"],
                            data["商品英文名"],
                            sell_min,
                            sell_num,
                            None, None,
                            data["类型"],
                            data["品质"],
                            data["稀有度"],
                            current_time,
                            current_time
                        ))
                    cursor.executemany(insert_sql, insert_params)
                    print(f"【写入线程】批量插入{len(insert_data)}条数据")

                if update_data:
                    update_sql = """
                                 UPDATE vic_skins
                                 SET name        = %s,
                                     sell_min    = %s,
                                     sell_num    = %s,
                                     `group`     = %s,
                                     exterior    = %s,
                                     rarity      = %s,
                                     time_update = %s
                                 WHERE id = %s
                                 """
                    update_params = []
                    for data, record_id in update_data:
                        sell_min_str = data["人民币价格"].replace("¥", "").replace(",", "")
                        sell_min = float(sell_min_str) if sell_min_str and sell_min_str != "未获取到" else None
                        sell_num_str = re.sub(r'\D', '', data["在售数量"])
                        sell_num = int(sell_num_str) if sell_num_str else None
                        current_time = data["爬取时间"]

                        update_params.append((
                            data["商品名称"],
                            sell_min,
                            sell_num,
                            data["类型"],
                            data["品质"],
                            data["稀有度"],
                            current_time,
                            record_id
                        ))
                    cursor.executemany(update_sql, update_params)
                    print(f"【写入线程】批量更新{len(update_data)}条数据")

            conn.commit()
            print(f"【写入线程】批量处理完成（总{len(batch_data)}条）")
            return True

        except Exception as e:
            if 'conn' in locals() and conn:
                conn.rollback()
            print(f"【写入线程】批量处理尝试 {attempt + 1}/{max_retries} 失败: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(2, 4))
                continue
            print(f"【写入线程】批量处理失败，数据将重试")
            return False
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    return False


# -------------------------- 写入线程函数 --------------------------
def writer_thread():
    batch_buffer = []
    print(f"【写入线程】启动，进入等待状态...")
    while not exit_event.is_set():  # 仅在程序退出时才终止循环
        try:
            # 阻塞等待队列数据（无超时），没数据时会一直等待（真正的等待状态）
            data = DATA_QUEUE.get()
            if data:
                batch_buffer.append(data)
                # CSV写入逻辑保持不变
                with csv_lock:
                    category = data["类型"]
                    display_category = "印花" if category.startswith("印花-") else category
                    filename = f"zbt_crawl_results_印花.csv" if display_category == "印花" else f"zbt_crawl_results_{display_category}.csv"
                    headers = ["id", "商品名称", "商品英文名", "人民币价格", "在售数量", "类型", "品质", "稀有度",
                               "爬取时间"]

                    is_first = csv_first_write[category]
                    mode = "w" if is_first else "a"
                    with open(filename, mode, newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        if is_first:
                            writer.writeheader()
                            csv_first_write[category] = False
                        writer.writerow({
                            "id": "",
                            "商品名称": data["商品名称"] or "",
                            "商品英文名": data["商品英文名"],
                            "人民币价格": data["人民币价格"],
                            "在售数量": data["在售数量"],
                            "类型": data["类型"],
                            "品质": data["品质"],
                            "稀有度": data["稀有度"],
                            "爬取时间": data["爬取时间"]
                        })
                DATA_QUEUE.task_done()

            # 达到批量大小则写入数据库
            if len(batch_buffer) >= BATCH_SIZE:
                print(f"【写入线程】达到批量大小({BATCH_SIZE})，开始处理数据")
                success = batch_save_or_update(batch_buffer)
                if success:
                    batch_buffer = []
                    print(f"【写入线程】批量数据处理完成，继续等待新数据...")
                else:
                    print(f"【写入线程】批量处理失败，保留{len(batch_buffer)}条数据重试")
                    time.sleep(5)  # 失败后短暂等待再重试

            # 关键逻辑：队列空且缓冲区有数据时，强制处理剩余数据（无论是否达到批量）
            # 处理完后回到循环顶部，通过DATA_QUEUE.get()进入阻塞等待
            if DATA_QUEUE.empty() and batch_buffer:
                print(f"【写入线程】队列已空，开始处理剩余{len(batch_buffer)}条数据")
                success = batch_save_or_update(batch_buffer)
                if success:
                    batch_buffer = []
                    print(f"【写入线程】剩余数据处理完成，进入等待状态...")  # 处理完进入等待
                else:
                    print(f"【写入线程】剩余数据处理失败，将重试")
                    time.sleep(5)  # 失败后重试

        except Exception as e:
            # 只处理非预期异常（无超时机制，不会有超时异常）
            print(f"【写入线程】处理异常: {str(e)}")
            continue

    # 程序退出时的最终处理（确保最后剩余数据被处理）
    if batch_buffer:
        print(f"【写入线程】程序退出，处理最后剩余{len(batch_buffer)}条数据")
        for attempt in range(5):
            if batch_save_or_update(batch_buffer):
                batch_buffer = []
                print(f"【写入线程】最后剩余数据处理成功")
                break
            else:
                print(f"【写入线程】最后剩余数据尝试 {attempt + 1}/5 失败，重试")
                time.sleep(3)
        if batch_buffer:
            print(f"【写入线程】警告：{len(batch_buffer)}条数据最终未写入")
    print(f"【写入线程】已退出（程序终止）")


# -----------------------------------------------------------------------------------
def save_invalid_data_to_csv(invalid_data):
    print("正在记录异常数据:", invalid_data)
    if not invalid_data:
        return
    filename = "zbt_invalid_data_log.csv"
    headers = ["记录时间", "分类", "页面URL", "无效原因", "商品名称", "商品英文名", "页码"]

    with invalid_data_lock:
        try:
            with open(filename, "r", encoding="utf-8") as f:
                has_header = True
        except FileNotFoundError:
            has_header = False

        mode = "a" if has_header else "w"
        with open(filename, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not has_header:
                writer.writeheader()
            writer.writerow(invalid_data)


def extract_quality(product_name):
    if not product_name:
        return "none"
    match = re.search(r'\(([^)]+)\)$', product_name.strip())
    return match.group(1).strip() if match else "none"


def process_detail_page(url, current_time, item_info, pattern, category, max_retries=3):
    for attempt in range(max_retries):
        try:
            product_name = item_info.get('name', '').strip()
            page_num = item_info.get('page', 1)

            invalid_data = {
                "记录时间": current_time,
                "分类": category,
                "页面URL": url,
                "无效原因": "",
                "商品名称": product_name or "【无名称】",
                "商品英文名": "未获取到",
                "页码": page_num
            }

            display_category = "印花" if category.startswith("印花-") else category
            item_data = {
                "商品名称": product_name,
                "商品英文名": "未获取到",
                "人民币价格": item_info.get('cn_price', ''),
                "类型": display_category,
                "稀有度": "未获取到",
                "在售数量": "未获取到",
                "品质": extract_quality(product_name),
                "爬取时间": current_time,
                "页码": page_num
            }

            session = create_session()
            headers = get_headers()
            cookies = get_random_cookies()
            time.sleep(random.uniform(0.5, 1.5))

            detail_resp = session.get(url, cookies=cookies, headers=headers)
            detail_resp.encoding = 'utf-8'

            if detail_resp.status_code != 200:
                if attempt < max_retries - 1:
                    print(
                        f"详情页请求尝试 {attempt + 1}/{max_retries} 失败，状态码: {detail_resp.status_code}，URL: {url}")
                    time.sleep(random.uniform(2, 4))
                    continue
                invalid_data["无效原因"] = f"请求失败，状态码: {detail_resp.status_code}"
                save_invalid_data_to_csv(invalid_data)
                print(f"【无效数据】{invalid_data['无效原因']}，URL: {url}")
                return None

            if detail_resp.status_code == 429:
                retry_after = int(detail_resp.headers.get('Retry-After', 5))
                time.sleep(retry_after)
                cookies = get_random_cookies()
                headers['user-agent'] = ua.random
                detail_resp = session.get(url, cookies=cookies, headers=headers)
                if detail_resp.status_code != 200:
                    if attempt < max_retries - 1:
                        print(f"详情页限流重试 {attempt + 1}/{max_retries} 失败，URL: {url}")
                        time.sleep(random.uniform(2, 4))
                        continue
                    invalid_data["无效原因"] = f"请求频繁，重试后仍失败，状态码: {detail_resp.status_code}"
                    save_invalid_data_to_csv(invalid_data)
                    print(f"【无效数据】{invalid_data['无效原因']}，URL: {url}")
                    return None

            detail_soup = BeautifulSoup(detail_resp.text, 'html.parser')

            english_name = "未获取到"
            steam_link = detail_soup.select_one('div.steam-price a[href*="steamcommunity.com/market/listings/730/"]')
            if steam_link and 'href' in steam_link.attrs:
                match = re.search(pattern, steam_link['href'])
                if match:
                    english_name = match.group(1).strip()
            if english_name == "未获取到":
                title_tag = detail_soup.select_one('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    title_match = re.search(r'^([^|]+)\|', title_text)
                    if title_match:
                        english_name = title_match.group(1).strip()
            item_data["商品英文名"] = english_name
            invalid_data["商品英文名"] = english_name

            if not product_name and english_name == "未获取到":
                invalid_data["无效原因"] = "无商品名称且无英文名，无法识别"
                save_invalid_data_to_csv(invalid_data)
                print(f"【无效数据】{invalid_data['无效原因']}，URL: {url}")
                return None

            if not item_data["人民币价格"] or item_data["人民币价格"] == "未获取到":
                price_container = detail_soup.select_one('span.text-usd.f16')
                if price_container:
                    price_text = re.sub(r'\s+', ' ', price_container.text.strip())
                    price_text_clean = re.sub(r',', '', price_text)
                    cn_price_match = re.search(r'¥\s*(\d+(?:\.\d+)?)', price_text_clean)
                    if cn_price_match:
                        item_data["人民币价格"] = f"¥{cn_price_match.group(1)}"
                if not item_data["人民币价格"] or item_data["人民币价格"] == "未获取到":
                    invalid_data["无效原因"] = "无法提取有效价格"
                    save_invalid_data_to_csv(invalid_data)
                    print(f"【无效数据】{invalid_data['无效原因']}，URL: {url}")
                    return None

            leixing_tag = detail_soup.find("ul", attrs={"class": "list-inline"})
            if leixing_tag:
                leixing_text = re.sub(r'\s+', ' ', leixing_tag.text.strip())
                leixing_text = re.sub(r' : ', ':', leixing_text)
                parts = leixing_text.split()
                for part in parts:
                    if part.startswith('类型:'):
                        item_data["类型"] = part.split(':', 1)[1]
                    if part.startswith('稀有度:'):
                        item_data["稀有度"] = part.split(':', 1)[1]

            zaishu_tag = detail_soup.find("ul", attrs={"class": "nav nav-tabs nav-tabs-base"})
            if zaishu_tag:
                zaishoushuliang_tag = zaishu_tag.find("span", attrs={"class": "text-dark"})
                if zaishoushuliang_tag:
                    zaishoushuliang = re.sub(r'[()]', '', zaishoushuliang_tag.text.strip())
                    zaishoushuliang = re.sub(r'件.*', '件', zaishoushuliang)
                    item_data["在售数量"] = zaishoushuliang

            DATA_QUEUE.put(item_data, block=True, timeout=30)
            print(
                f"【爬取线程】数据已放入队列 - {item_data['商品名称'] or '【无名称】'}，当前队列大小: {DATA_QUEUE.qsize()}")
            return item_data

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"处理详情页尝试 {attempt + 1}/{max_retries} 失败: {str(e)}，URL: {url}")
                time.sleep(random.uniform(2, 4))
                continue
            invalid_data["无效原因"] = f"处理异常: {str(e)}"
            save_invalid_data_to_csv(invalid_data)
            print(f"【无效数据】{invalid_data['无效原因']}，URL: {url}")
            return None

    return None


# -------------------------- 爬取函数（支持重试失败页面） --------------------------
def crawl_category(category, first_url, page_url_template, pages_to_crawl, is_retry=False):
    """
    爬取指定分类的指定页面（支持首次爬取和重试）
    :param pages_to_crawl: 需要爬取的页码列表（如[1,2,3]）
    :param is_retry: 是否为重试模式（用于日志区分）
    """
    global crawl_id
    with running_lock:
        if category_running[category]:
            return
        category_running[category] = True

    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        total_data_count = 0
        pattern = r'market/listings/730/(.*)'
        success_pages = 0
        session = create_session()

        time.sleep(random.uniform(1, 5))
        mode = "重试" if is_retry else "首次"
        print(f"【{mode}爬取分类】{category}，计划爬取页码: {pages_to_crawl}")

        for page in pages_to_crawl:
            url = first_url if page == CATEGORIES[category][3] else page_url_template.format(page=page)
            page_success = False

            for page_retry in range(3):  # 单页内部重试3次
                try:
                    headers = get_headers()
                    cookies = get_random_cookies()
                    time.sleep(random.uniform(2, 4))

                    response = session.get(url, cookies=cookies, headers=headers)
                    response.encoding = 'utf-8'

                    if response.status_code == 429:
                        retry_after = int(response.headers.get('Retry-After', 10))
                        print(
                            f"【列表页限流】{mode}爬取{category}第{page}页，尝试 {page_retry + 1}/3，将重试after {retry_after}秒")
                        time.sleep(retry_after)
                        cookies = get_random_cookies()
                        headers['user-agent'] = ua.random
                        response = session.get(url, cookies=cookies, headers=headers)

                    if response.status_code != 200:
                        invalid_data = {
                            "记录时间": current_time,
                            "分类": category,
                            "页面URL": url,
                            "无效原因": f"列表页请求失败，状态码: {response.status_code}，尝试 {page_retry + 1}/3",
                            "商品名称": "N/A",
                            "商品英文名": "N/A",
                            "页码": page
                        }
                        save_invalid_data_to_csv(invalid_data)
                        print(f"【无效数据】{invalid_data['无效原因']}")
                        if page_retry < 2:
                            time.sleep(random.uniform(3, 5))
                            continue
                        else:
                            break

                    # 解析列表页
                    soup = BeautifulSoup(response.text, 'html.parser')
                    case_detail_urls = []
                    for link in soup.select('a.market-item'):
                        href = link.get('href')
                        if href and not href.startswith('http'):
                            href = f'https://www.zbt.com{href}'
                        if href:
                            case_detail_urls.append(href)

                    h5_list = soup.find_all('h5')
                    price_spans = soup.find_all('span', class_='f16 text-usd')

                    print(
                        f"【{mode}爬取列表页统计】{category}第{page}页 - 链接数: {len(case_detail_urls)}, 名称数: {len(h5_list)}, 价格数: {len(price_spans)}")

                    item_infos = []
                    for i in range(min(len(case_detail_urls), len(h5_list), len(price_spans))):
                        raw_name = h5_list[i].text.strip()
                        clean_name = re.sub(r'\s+', ' ', raw_name).strip()
                        raw_price = price_spans[i].text.strip()
                        raw_price_clean = re.sub(r'\s+', ' ', raw_price)
                        raw_price_clean = re.sub(r',', '', raw_price_clean)
                        cn_price_match = re.search(r'¥\s*(\d+(?:\.\d+)?)', raw_price_clean)
                        cn_price = f"¥{cn_price_match.group(1)}" if cn_price_match else "未获取到"

                        item_infos.append({
                            'page': page,
                            'name': clean_name,
                            'cn_price': cn_price
                        })

                    if not item_infos:
                        print(f"【{mode}爬取列表页无数据】{category}第{page}页没有找到有效数据")
                        page_success = True  # 无数据也算成功（避免重复重试空页面）
                        break

                    max_workers = max(1, min(3, len(item_infos)))
                    page_data_count = 0
                    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = []
                        for url, item_info in zip(case_detail_urls, item_infos):
                            futures.append(executor.submit(
                                process_detail_page,
                                url, current_time, item_info, pattern, category
                            ))
                        for future in concurrent.futures.as_completed(futures):
                            result = future.result()
                            if result:
                                total_data_count += 1
                                page_data_count += 1

                    success_pages += 1
                    print(f"【{mode}爬取列表页完成】{category}第{page}页，成功处理{page_data_count}条数据")
                    page_success = True

                    # 控制爬取速度
                    if page % 5 == 0:
                        time.sleep(random.uniform(10, 15))
                    else:
                        time.sleep(random.uniform(2, 4))
                    break

                except Exception as e:
                    invalid_data = {
                        "记录时间": current_time,
                        "分类": category,
                        "页面URL": url,
                        "无效原因": f"列表页处理异常: {str(e)}，尝试 {page_retry + 1}/3",
                        "商品名称": "N/A",
                        "商品英文名": "N/A",
                        "页码": page
                    }
                    save_invalid_data_to_csv(invalid_data)
                    print(f"【无效数据】{invalid_data['无效原因']}")
                    if page_retry < 2:
                        time.sleep(random.uniform(5, 8))
                        continue
                    else:
                        break

            # 记录失败页面（单页3次尝试均失败）
            if not page_success:
                print(f"【{mode}爬取列表页最终失败】{category}第{page}页经多次重试仍失败")
                with failed_pages_lock:
                    # 记录重试次数，超过MAX_RETRIES则不再重试
                    current_retry = failed_pages[category].get(page, 0)
                    if current_retry < MAX_RETRIES:
                        failed_pages[category][page] = current_retry + 1
                    else:
                        print(f"【超过最大重试次数】{category}第{page}页已重试{MAX_RETRIES}次，停止重试")
            else:
                # 移除已成功的页面（如果之前记录过失败）
                with failed_pages_lock:
                    if page in failed_pages[category]:
                        del failed_pages[category][page]

        print(
            f"【{mode}爬取分类完成】{category} - 计划{len(pages_to_crawl)}页，实际成功{success_pages}页，共处理{total_data_count}条数据")
        time.sleep(random.uniform(15, 25))

    except Exception as e:
        print(f"【分类线程异常】{category}，错误: {str(e)}")
        time.sleep(30)
    finally:
        with running_lock:
            category_running[category] = False


# -------------------------- 重试失败页面 --------------------------
def retry_failed_pages():
    """检查失败页面并进行重试，直到无失败页面或达到最大重试次数"""
    retry_round = 1
    while True:
        # 获取当前失败页面（复制一份避免线程冲突）
        with failed_pages_lock:
            current_failed = {k: v.copy() for k, v in failed_pages.items() if v}

        if not current_failed:
            print(f"【所有页面爬取完成】无失败页面，重试结束")
            break

        if retry_round > MAX_RETRIES:
            print(f"【达到最大重试轮次】已完成{MAX_RETRIES}轮重试，剩余失败页面不再处理")
            # 打印最终失败页面
            with failed_pages_lock:
                for category, pages in failed_pages.items():
                    if pages:
                        print(f"【最终失败页面】{category}: {list(pages.keys())}")
            break

        print(f"\n【开始第{retry_round}轮重试】失败页面统计: { {k:list(v.keys()) for k,v in current_failed.items()} }")

        # 多线程重试失败页面
        threads = []
        for category, pages in current_failed.items():
            # 获取分类配置
            first_url, page_url, _, _ = CATEGORIES[category]
            # 重试该分类的失败页码
            thread = threading.Thread(
                target=crawl_category,
                args=(category, first_url, page_url, list(pages.keys()), True),  # is_retry=True
                name=f"Retry-Thread-{category}"
            )
            threads.append(thread)
            thread.start()
            time.sleep(random.uniform(1, 3))  # 错开启动时间

        # 等待本轮重试完成
        for thread in threads:
            thread.join()

        retry_round += 1


# -------------------------- 全量爬取（首次+重试） --------------------------
def crawl_all_categories():
    global crawl_id
    with id_lock:
        crawl_id = get_max_id() + 1
    print("【开始首次全分类爬取】")

    # 首次爬取：按分类配置的所有页面
    threads = []
    for category, (first_url, page_url, total_pages, start_page) in CATEGORIES.items():
        # 计算需要爬取的页码范围
        end_page = start_page + total_pages - 1
        pages_to_crawl = list(range(start_page, end_page + 1))
        thread = threading.Thread(
            target=crawl_category,
            args=(category, first_url, page_url, pages_to_crawl, False),  # is_retry=False
            name=f"First-Thread-{category}"
        )
        threads.append(thread)
        thread.start()
        time.sleep(random.uniform(0.5, 2))

    # 等待首次爬取完成
    for thread in threads:
        thread.join()

    print("【首次全分类爬取结束】开始处理失败页面...")
    # 重试失败页面
    retry_failed_pages()

    print("【全量爬取流程完成】等待写入线程处理剩余数据...")
    # 等待队列数据处理完毕
    DATA_QUEUE.join()
    # 触发退出事件，让写入线程处理剩余数据
    exit_event.set()


# -------------------------- 优雅退出与主函数 --------------------------
def handle_exit(signum, frame):
    print("\n【程序收到退出信号】开始优雅退出...")
    exit_event.set()
    DATA_QUEUE.join()
    print("【程序已安全退出】")
    exit(0)


def main():
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # 启动写入线程
    writer = threading.Thread(target=writer_thread, name="Writer-Thread")
    writer.daemon = False
    writer.start()

    # 定时任务：每天凌晨2点执行
    schedule.every().day.at("02:00").do(crawl_all_categories)

    # 立即执行一次
    crawl_all_categories()

    # 保持运行
    while not exit_event.is_set():
        schedule.run_pending()
        time.sleep(60)

    writer.join()


if __name__ == "__main__":
    main()