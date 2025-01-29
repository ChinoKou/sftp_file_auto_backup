import os
import json
import stat
import time
import asyncio
import aiofiles
import asyncssh
from tqdm import tqdm
from loguru import logger
from datetime import datetime
from traceback import print_exc


class SFTPDownloader:
    def __init__(self, config_file="config.json", log_dir="./logs"):
        self.config_file = config_file
        self.log_dir = log_dir
        self.downloaded_files = {}
        self.chunk_size = 512 * 512
        self.ignore_directories = set()
        self.last_downloaded_file_path = None

        self.init_logging()

    def init_logging(self):
        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)

        start_time = time.strftime("%Y-%m-%d", time.localtime())
        log_file_name = os.path.join(self.log_dir, f"{start_time}.log")
        downloaded_log_file_name = os.path.join(self.log_dir, f"{time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime())} downloaded.json")

        logger.add(log_file_name, rotation="5 MB", level="DEBUG")
        self.downloaded_log_file_name = downloaded_log_file_name

    def load_config(self):
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def save_config(self, config):
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
        except IOError as e:
            logger.error(f"保存配置文件失败: {e}")
            raise

    def error_handler(self, e):
        if isinstance(e, KeyboardInterrupt):
            logger.warning("用户终止程序运行")
        elif e is not None:
            logger.warning("捕获到程序运行异常！")
            logger.info("正在尝试保存报错文件...")

            error_log_file = os.path.join(self.log_dir, f"{time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime())} error.log")
            try:
                with open(error_log_file, "w", encoding="utf-8") as f:
                    print_exc(file=f)
            except Exception as inner_e:
                logger.error(f"保存错误日志失败: {inner_e}")

        if self.last_downloaded_file_path and os.path.exists(self.last_downloaded_file_path):
            logger.warning("正在尝试删除最后一次下载的文件...")
            try:
                os.remove(self.last_downloaded_file_path)
                logger.warning(f"已删除文件: \"{self.last_downloaded_file_path}\"")
            except FileNotFoundError:
                logger.warning("文件暂未创建，无需删除")
            except PermissionError:
                logger.warning("文件正在被其他程序使用，无法删除")

    def save_downloaded_log(self):
        try:
            with open(self.downloaded_log_file_name, "w", encoding="utf-8") as f:
                json.dump(self.downloaded_files, f, indent=4, ensure_ascii=False)
        except IOError as e:
            logger.error(f"保存下载记录失败: {e}")

    async def download(self, sftp, remote_path, local_path, ignore_directories):
        need_items = {}

        try:
            filenames = await sftp.listdir(remote_path)
        except asyncssh.SFTPFailure as e:
            logger.error(f"无法列出目录 {remote_path}: {e}")
            return

        os.makedirs(local_path, exist_ok=True)

        logger.info("开始遍历远程文件夹")
        items_num = [0, 0, 0, 0, 0]

        for filename in filenames:
            items_num[0] += 1

            if filename in ('.', '..'):
                items_num[0] -= 1
                continue

            if filename in ignore_directories:
                logger.warning(f"文件夹: \"{filename}\" 已被忽略")
                items_num[3] += 1
                continue

            remote_item_path = f"{remote_path}/{filename}"

            try:
                attr = await sftp.stat(remote_item_path)
            except asyncssh.SFTPFailure as e:
                logger.error(f"无法获取文件 {filename} 的属性！")
                items_num[3] += 1
                continue

            remote_mtime = attr.mtime
            remote_size = attr.size
            remote_is_dir = stat.S_ISDIR(attr.permissions)
            local_item_path = os.path.join(local_path, filename)

            if os.path.exists(local_item_path):
                local_mtime = os.stat(local_item_path).st_mtime
            else:
                local_mtime = 0

            if not remote_is_dir and remote_mtime <= local_mtime:
                logger.info(f"跳过文件：\"{filename}\"")
                items_num[3] += 1
            else:
                if remote_is_dir:
                    logger.info(f"文件夹：  \"{filename}\"")
                    items_num[1] += 1
                else:
                    logger.info(f"文件：    \"{filename}\"")
                    items_num[2] += 1

                need_items[filename] = (
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

        for filename, value in need_items.items():
            if value[3]:
                logger.info("\n")
                logger.info(f"进入文件夹: \"{value[0]}\"")
                await self.download(sftp, value[0], value[4], ignore_directories)
            else:
                self.last_downloaded_file_path = value[4]
                self.downloaded_files[f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"] = value[0]
                items_num[4] += 1

                logger.info("------------------------------------------------------------------------------------------")
                logger.info(f"开始下载文件 \"{value[6]}\"")
                logger.info(f"远程文件最后修改时间: {datetime.fromtimestamp(value[1])}")

                if value[5] > 10:
                    logger.info(f"本地文件最后修改时间: {datetime.fromtimestamp(value[5])}")

                logger.info(f"文件大小: {value[2] / 1024:.2f} KB")

                if items_num[2] != 1:
                    logger.info(f"正在下载第 {items_num[4]} / {items_num[2]} 个文件")

                if value[2] <= self.chunk_size:
                    await sftp.get(value[0], value[4])
                else:
                    try:
                        async with sftp.open(value[0], 'rb') as remote_file:
                            async with aiofiles.open(value[4], 'wb') as local_file:
                                with tqdm(total=value[2], unit='B', unit_scale=True, desc=f"E:{value[0]}") as pbar:
                                    while True:
                                        data = await remote_file.read(self.chunk_size)
                                        if not data:
                                            break
                                        await local_file.write(data)
                                        pbar.update(len(data))
                    except Exception as e:
                        self.error_handler(e)

                if os.path.getsize(value[4]) == value[2]:
                    logger.success(f"成功下载至 \"{value[4]}\"")
                    logger.info("------------------------------------------------------------------------------------------\n")
                else:
                    logger.error(f"下载 \"{value[0]}\" 失败！")
                    self.error_handler(None)
                    time.sleep(1)
                    logger.info("------------------------------------------------------------------------------------------\n")

    async def main(self):
        logger.info("定时备份启动")
        logger.info("正在加载配置文件...")

        try:
            config = self.load_config()
            logger.info(f"SFTP服务器: {config['hostname']}:{config['port']}")
            logger.info(f"用户: {config['username']}")
            local_root = os.path.abspath(config["local_path"])
            logger.info(f"远程备份路径: {config['remote_path']}")
            logger.info(f"本地备份路径: {local_root}")
            logger.info(f"忽略的文件夹: {config['ignore_directories']}")
            logger.success("配置文件加载成功！")

            if not os.path.exists(config["local_path"]):
                logger.warning("备份文件夹不存在！")
                logger.info("正在创建备份文件夹...")
                os.makedirs(config["local_path"])
                if os.path.exists(config["local_path"]):
                    logger.success("创建成功！")

            self.ignore_directories = set(config["ignore_directories"])

            logger.info("正在连接SFTP服务器...")
            async with asyncssh.connect(
                host=config["hostname"],
                port=config["port"],
                username=config["username"],
                password=config["password"],
                known_hosts=None
            ) as conn:
                async with conn.start_sftp_client() as sftp:
                    logger.success("连接成功！")
                    logger.info("即将启动下载线程")
                    await self.download(sftp, config["remote_path"], local_root, self.ignore_directories)

        except Exception as e:
            logger.error("配置文件加载失败！")
            raise e

        finally:
            self.save_config(config)

    def run(self):
        try:
            asyncio.run(self.main())
        except KeyboardInterrupt as e:
            self.error_handler(e)
        except Exception as e:
            logger.error(f"遇到未知错误！")
            self.error_handler(e)
        finally:
            download_num = len(self.downloaded_files)
            if download_num != 0:
                logger.info(f"本次下载了 {download_num} 个文件")
                logger.info("正在保存下载记录...")
                self.downloaded_files["本次运行已下载文件数"] = download_num
                self.save_downloaded_log()
            logger.info("程序将在5s后退出")
            time.sleep(5)


if __name__ == "__main__":
    downloader = SFTPDownloader()
    try:
        downloader.run()
    except KeyboardInterrupt:
        logger.info("检测到用户强制终止，退出程序")