import requests
import re
import argparse
from threading import Timer
from redis import Redis
from multiprocessing.dummy import Pool
import logging

# 设置参数
parser = argparse.ArgumentParser(description="Test for argparse")
parser.add_argument('-url','-u', help="指定爬虫开始地址, 必要参数", required=True)
parser.add_argument('-deep','-d', help="爬虫深度, 可选参数，默认值为1", default=1, type=int)
parser.add_argument('-file','-f', help="日志文件路径, 可选参数， 默认值为spider.log", default="./spider.log")
parser.add_argument('-download_file','-df', help="爬虫文件夹路径, 可选参数， 默认值为当前路径", default="./")
parser.add_argument('--concurrency','-c', help="指定线程数，可选参数。默认值为1", default=1, type=int)
parser.add_argument('--key','-k', help="页面内容关键词，可选参数。默认值为所有页面", default="")
args = parser.parse_args()

# 解析参数
url = args.url
deep = args.deep
file = args.file
concurrency = args.concurrency
key = args.key
d_file = args.download_file

# 设置爬虫类
class crawl():
    def __init__(self, url, key, file, concurrency, deep,d_file ):
        """
        构造函数：
        定义相关爬虫信息
        """
        self.url = list() # 起始页url
        self.url.append(url)
        self.key = key
        self.path = file  # 日志文件地址
        self.concurrency = concurrency
        self.deep = deep
        self.d_file = d_file # 爬虫文件地址
        self.conn = Redis(host="localhost", port=6379)
        self.conn.set("crawl_id", 0)
        self.conn.set("download_id", 0)
        self.t = None
        # 设置日志文件
        logging.basicConfig(filename=self.path, level=logging.INFO, format='%(levelname)s:%(asctime)s:%(message)s', ) # 日志文件配置
        # 匹配网站首页
        self.url_index = re.findall(r'[http|https]+://.*?/', str(self.url[0]))
        # 请求头
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
        }

    def printf(self):
        """
        打印函数：
        每十秒打印一次爬虫信息
        return : None
        """
        download_id = self.conn.get("download_id")
        crawl_id = self.conn.get("crawl_id")
        print("已爬取页面%d,已下载页面%d"%(int(crawl_id),int(download_id)))
        self.t = Timer(10,self.printf)
        self.t.start()

    def get_request(self,url):
        """
        请求函数:
        对url发起get请求
        返回请求的响应数据
        return : page_text
        """
        ex = self.conn.sadd("html_url", url)
        if ex == 1:
            try:
                session = requests.session()
                page_text = session.get(url=url, headers=self.header)
                page_text.encoding = 'utf-8'
                logging.info("已请求的url:%s"%url) # 添加日志信息
                self.conn.incr("crawl_id")
                return page_text.text
            except Exception as a:
                logging.error("请求错误，错误类型为：%s, 错误url为%s"%(a, url))
        return 0

    def get_text(self, texts):
        """
        页面解析:
        对请求的响应数据进行页面解析
        解析出页面的所有url和关键字
        返回解析出来的url列表
        return : url_list
        """
        url_list = []
        try:
            if texts:
                key_list = re.findall(self.key, str(texts))
                if key_list:
                    download_id = self.conn.incr("download_id")
                    path = self.d_file + str(download_id) + ".html"
                    with open(path, encoding='utf-8', mode='a') as f:
                        f.write(str(texts))
                tag = re.findall(r'<a href=".*?"', str(texts))
                for i in tag:
                    if "http" in i:
                        url_list.append(i[9:-1])
                    else:
                        url_list.append(self.url_index[0] + i[9:-1])
        except Exception as a:
            logging.error("请求错误，错误类型为：%s" % a)
        return url_list

    def pool_map(self, u, d, c ):
        """
        线程池:
        设置线程池并递归调用进行深度爬虫
        return : None
        """
        if d == 0:
            return 0
        urls = u
        pool = Pool(c)
        result_list = pool.map(self.get_request, urls)
        result_data = pool.map(self.get_text, result_list)
        d -= 1
        for i in result_data:
            self.pool_map(i, d, self.concurrency)


    def run(self):
        """
        启动爬虫函数：
        负责启动爬虫
        return : 0
        """
        self.printf()
        self.pool_map(self.url, self.deep, self.concurrency)
        download_id = self.conn.get("download_id")
        crawl_id = self.conn.get("crawl_id")
        print("已爬取页面%d,已下载页面%d" % (int(crawl_id), int(download_id)))
        print("爬虫结束")
        self.t.cancel()

if __name__ == '__main__':
    c = crawl(url, key, file, concurrency, deep, d_file)
    c.run()
