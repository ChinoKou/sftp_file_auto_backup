import os
import json
import stat
import time
import paramiko
import paramiko.ssh_exception
from tqdm import tqdm
from loguru import logger
from datetime import datetime


if not os.path.exists("logs"):
    os.mkdir("logs")

chunk_size = 512 * 512
start_time = time.strftime("%Y-%m-%d", time.localtime())
log_dir = os.path.abspath("./logs")
log_file = os.path.join(log_dir, f"{start_time}.log")
downloaded_log_file = os.path.join(log_dir, f"{time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())} downloaded.json")
logger.add(log_file, rotation = "10 MB", level = "DEBUG")

def load_config():
    global config
    with open("sftp_config.json", 'r') as f:
        config = json.loads(f.read())

def save_config():
    with open("sftp_config.json", 'w') as f:
        f.write(json.dumps(config, indent = 4))

def last_downloaded_delete():
    if is_downloaded:
        logger.warning("捕获到程序运行异常！")
        logger.warning("正在删除最后一次下载的文件...")
        os.remove(local_item_path)
        logger.warning(f"已删除文件: \"{local_item_path}\"")

def save_downloaded_log():
    with open(downloaded_log_file, "w", encoding = "utf-8") as f:
        f.write(json.dumps(downloaded_files, indent = 4, ensure_ascii = False))

def download_file(sftp, remote_path, local_path):
    """下载单个文件"""
    global is_downloaded

    # 获取远程文件的属性
    remote_stat = sftp.stat(remote_path)
    remote_mtime = remote_stat.st_mtime
    remote_size = remote_stat.st_size

    # 获取本地文件的属性（如果存在）
    if os.path.exists(local_path):
        local_stat = os.stat(local_path)
        local_mtime = local_stat.st_mtime
    else:
        local_mtime = 0

    # 比较修改时间
    if remote_mtime > local_mtime:
        is_downloaded = True
        logger.info(f"尝试下载文件 \"{remote_path}\"")
        logger.info(f"文件大小: {remote_size / 1024:.2f} KB")
        if remote_size <= chunk_size:
            with tqdm(total = remote_size, unit = 'B', unit_scale = True, desc = remote_path) as pbar:
                sftp.get(remote_path, local_path)
                pbar.update(os.stat(local_path).st_size)
        else:
            with sftp.open(remote_path, 'rb') as remote_file:
                with open(local_path, 'wb') as local_file:
                    with tqdm(total = remote_size, unit = 'B', unit_scale = True, desc = remote_path) as pbar:
                        while True:
                            data = remote_file.read(chunk_size)  # 分块下载
                            if not data:
                                break
                            local_file.write(data)
                            pbar.update(len(data))
        logger.success(f"成功下载 \"{remote_path}\"")
        logger.success(f"至 \"{local_path}\"")
        downloaded_files[f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}"] = remote_path
    else:
        logger.warning(f"跳过 \"{remote_path}\"")


def download_directory(sftp, remote_path, local_path, skip_directories):
    """递归下载远程目录及其所有子目录和文件"""
    global local_item_path
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    
    items = sftp.listdir_attr(remote_path)
    # logger.info(f"列出路径 \"{remote_path}\" 下的文件: ")
    # logger.info(items)
    
    for item in items:
        remote_item_path = os.path.join(remote_path, item.filename).replace('\\', '/')
        local_item_path = os.path.join(local_path, item.filename)

        if stat.S_ISDIR(item.st_mode):  # 如果是目录
            if item.filename in skip_directories:  # 如果指定了跳过文件夹，则跳过该文件夹
                logger.warning(f"跳过文件夹: \"{remote_item_path}\"")
                continue

            logger.info(f"进入文件夹: \"{remote_item_path}\"")
            download_directory(sftp, remote_item_path, local_item_path, skip_directories)

        else:  # 如果是文件
            download_file(sftp, remote_item_path, local_item_path)

if __name__ == "__main__":
    global config
    load_config()

    global downloaded_files
    downloaded_files = {}

    global is_downloaded
    is_downloaded = False

    # 使用绝对路径
    local_root = os.path.abspath(config['local_path'])
    remote_root = config['remote_path']
    skip_directories = config.get('skip_directories', [])

    try:
        client = paramiko.SSHClient()  # 获取SSHClient实例
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=config['hostname'],
            port=config['port'],
            username=config['username'],
            password=config['password']
        )  # 连接SSH服务端
        transport = client.get_transport()  # 获取Transport实例

        # 创建sftp对象
        sftp = paramiko.SFTPClient.from_transport(transport)

        # 下载
        download_directory(sftp, remote_root, local_root, skip_directories)

    # socket.gaierror: [Errno 11002] getaddrinfo failed
    except paramiko.ssh_exception.SSHException as e:
        logger.error("SFTP服务器连接终止！")
        logger.exception(e)
        last_downloaded_delete()
    except KeyboardInterrupt:
        logger.warning("用户终止程序运行")
        last_downloaded_delete()
    except Exception as e:
        logger.error(f"遇到未知错误:")
        logger.exception(e)
        last_downloaded_delete()
    finally:
        client.close()
        logger.info(f"本次下载了 {len(downloaded_files)} 个文件")
        logger.info("正在保存下载记录...")
        save_config()
        downloaded_files["本次运行已下载文件数"] = len(downloaded_files)
        save_downloaded_log()