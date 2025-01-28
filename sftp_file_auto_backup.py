import os
import json
import stat
import time
import paramiko
import paramiko.ssh_exception
from tqdm import tqdm
from loguru import logger
from datetime import datetime


def load_config():
    with open("config.json", 'r') as f:
        config = json.loads(f.read())
        return config

def save_config(config):
    with open("config.json", 'w') as f:
        f.write(json.dumps(config, indent = 4))

def last_downloaded_delete():
    if len(DOWNLOADED_FILES) > 0:
        logger.warning("捕获到程序运行异常！")
        logger.warning("正在尝试删除最后一次下载的文件...")

        try:
            os.remove(LAST_DOWNLOADED_FILE_PATH)
            logger.warning(f"已删除文件: \"{LAST_DOWNLOADED_FILE_PATH}\"")

        except FileNotFoundError:
            logger.warning("文件暂未创建，无需删除")

def save_downloaded_log():
    with open(DOWNLOADED_LOG_FILE_NAME, "w", encoding = "utf-8") as f:
        f.write(json.dumps(DOWNLOADED_FILES, indent = 4, ensure_ascii = False))

def download(remote_path, local_path, ignore_directories):
    need_items = {}
    items = sftp.listdir_attr(remote_path)

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    logger.info("开始遍历远程文件夹")
    items_num = [0, 0, 0, 0, 0]

    for item in items:
        items_num[0] += 1
        filename = item.filename

        if filename in ignore_directories:
            logger.warning(f"文件夹: \"{filename}\" 已被忽略")
            items_num[3] += 1
            continue

        remote_item_path = os.path.join(remote_path, filename).replace('\\', '/')
        remote_stat = sftp.stat(remote_item_path)
        remote_mtime = remote_stat.st_mtime
        remote_size = remote_stat.st_size
        remote_is_dir = stat.S_ISDIR(remote_stat.st_mode)
        local_item_path = os.path.join(local_path, filename)

        if os.path.exists(local_item_path):
            local_stat = os.stat(local_item_path)
            local_mtime = local_stat.st_mtime
        else:
            local_mtime = 0

        if not remote_is_dir and not remote_mtime > local_mtime:
            logger.info(f"跳过文件：\"{filename}\"")
            items_num[3] += 1
        else:
            if remote_is_dir:
                logger.info(f"文件夹：  \"{filename}\"")
                items_num[1] += 1
            else:
                logger.info(f"文件：    \"{filename}\"")
                items_num[2] += 1
            need_items[item] = (
                remote_item_path,
                remote_mtime,
                remote_size,
                remote_is_dir,
                local_item_path,
                local_mtime,
                filename
            )

    logger.success("遍历完成！")
    logger.info(f"获取到   {items_num[0]} 个文件")
    if items_num[1]:
        logger.info(f"需要遍历 {items_num[1]} 个文件夹")
    if items_num[2]:
        logger.info(f"需要下载 {items_num[2]} 个文件")
    if items_num[3]:
        logger.warning(f"跳过下载 {items_num[3]} 个文件")
 
    for item, value in need_items.items():
        if value[3]:
            logger.info("\n")
            logger.info(f"进入文件夹: \"{value[0]}\"")
            download(value[0], value[4], ignore_directories)
        else:
            global LAST_DOWNLOADED_FILE_PATH

            LAST_DOWNLOADED_FILE_PATH = value[4]
            DOWNLOADED_FILES[f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}"] = value[0]
            items_num[4] += 1

            logger.info("------------------------------------------------------------------------------------------")
            logger.info(f"开始下载文件 \"{value[6]}\"")
            logger.info(f"远程文件最后修改时间: {datetime.fromtimestamp(value[1])}")

            if value[5] > 10:
                logger.info(f"本地文件最后修改时间: {datetime.fromtimestamp(value[5])}")

            logger.info(f"文件大小: {value[2] / 1024:.2f} KB")
            logger.info(f"正在下载第 {items_num[4]} / {items_num[2]} 个文件")

            if value[2] <= CHUNK_SIZE:
                sftp.get(value[0], value[4])
            else:
                with sftp.open(value[0], 'rb') as remote_file:
                    with open(value[4], 'wb') as local_file:
                        with tqdm(total = value[2], unit = 'B', unit_scale = True, desc = f"E:{value[0]}") as pbar:
                            while True:
                                data = remote_file.read(CHUNK_SIZE)
                                if not data:
                                    break
                                local_file.write(data)
                                pbar.update(len(data))

            if os.stat(value[4]).st_size == value[2]:
                logger.success(f"成功下载至   \"{value[4]}\"")
                logger.info("------------------------------------------------------------------------------------------\n")
            else:
                logger.error(f"下载 \"{value[0]}\" 失败！")
                last_downloaded_delete()
                time.sleep(1)

def main():
    logger.info("定时备份启动")
    logger.info("正在加载配置文件...")
    time.sleep(1)

    try:
        config = load_config()
        logger.info(f"SFTP服务器: {config["hostname"]}:{config["port"]}")
        time.sleep(0.25)
        logger.info(f"用户: {config["username"]}")
        time.sleep(0.25)
        local_root = os.path.abspath(config["local_path"])
        logger.info(f"远程备份路径: {config["remote_path"]}")
        time.sleep(0.25)
        logger.info(f"本地备份路径: {local_root}")
        time.sleep(0.25)
        logger.info(f"忽略的文件夹: {config["ignore_directories"]}")
        time.sleep(0.25)
        logger.success("配置文件加载成功！")

        if not os.path.exists(config["local_path"]):
            logger.warning("备份文件夹不存在！")
            logger.info("正在创建备份文件夹...")
            time.sleep(1)
            os.makedirs(config["local_path"])
            if os.path.exists(config["local_path"]):
                logger.success("创建成功！")

    except Exception as e:
        logger.error("配置文件加载失败！")
        raise e

    try:
        logger.info("正在连接SFTP服务器...")
        time.sleep(1)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname = config["hostname"],
            port = config["port"],
            username = config["username"],
            password = config["password"]
        )
        transport = client.get_transport()

        if transport.is_active():
            logger.success("连接成功！")
            global sftp
            sftp = paramiko.SFTPClient.from_transport(transport)
            logger.info("即将启动下载线程")
            time.sleep(1)
            download(config["remote_path"], local_root, config["ignore_directories"])

    except Exception as e:
        raise e

    finally:
        client.close()
        save_config(config)

def init():
    logger.debug("程序启动初始化")

    if not os.path.exists("logs"):
        os.mkdir("logs")

    global CHUNK_SIZE
    global DOWNLOADED_FILES
    global DOWNLOADED_LOG_FILE_NAME

    CHUNK_SIZE = 512 * 512
    START_TIME = time.strftime("%Y-%m-%d", time.localtime())
    LOG_DIR = os.path.abspath("./logs")
    LOG_FILE_NAME = os.path.join(LOG_DIR, f"{START_TIME}.log")
    DOWNLOADED_FILES = {}
    DOWNLOADED_LOG_FILE_NAME = os.path.join(LOG_DIR, f"{time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())} downloaded.json")

    logger.add(LOG_FILE_NAME, rotation = "5 MB", level = "DEBUG")

    try:
        main()

    except paramiko.AuthenticationException:
        logger.error("sftp服务器连接失败，尝试重新启动...   ")
        logger.exception("paramiko.AuthenticationException")
        last_downloaded_delete()
        time.sleep(1)

    except paramiko.ssh_exception.SSHException as e:
        logger.error("SFTP服务器连接终止！")
        logger.exception(e)
        last_downloaded_delete()
        time.sleep(1)

    except KeyboardInterrupt:
        logger.warning("用户终止程序运行")
        last_downloaded_delete()

    except Exception as e:
        logger.error(f"遇到未知错误:")
        logger.exception(e)
        last_downloaded_delete()
        time.sleep(1)

    finally:
        logger.info(f"本次下载了 {len(DOWNLOADED_FILES)} 个文件")
        logger.info("正在保存下载记录...")
        DOWNLOADED_FILES["本次运行已下载文件数"] = len(DOWNLOADED_FILES)
        save_downloaded_log()
        logger.info("程序将在5s后退出")
        time.sleep(5)

if __name__ == "__main__":
    init()