# -*- coding: utf-8 -*-
# 此程序用来抓取 的数据
import requests
import time
import random
import re
from multiprocessing.dummy import Pool
import csv
import json
import sys
from fake_useragent import UserAgent, FakeUserAgentError

class Spider(object):
	def __init__(self):
		self.date = '2000-01-01'
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
	
	def get_headers(self):
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
		               'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
		               'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
		               'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
		               'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
		               'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
		               'Opera/9.52 (Windows NT 5.0; U; en)',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
		               'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
		               'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'Host': 'www.shilladfs.com', 'Connection': 'keep-alive',
		           'User-Agent': user_agent,
		           'Referer': 'http://www.shilladfs.com/estore/kr/zh/Skin-Care/Basic-Skin-Care/Pack-Mask-Pack/p/3325351',
		           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
		           'Accept-Encoding': 'gzip, deflate, br',
		           'Accept-Language': 'zh-CN,zh;q=0.8'
		           }
		return headers

	def remove_emoji(self, text):
		emoji_pattern = re.compile(
			u"(\ud83d[\ude00-\ude4f])|"  # emoticons
			u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
			u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
			u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
			u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
			"+", flags=re.UNICODE)
		return emoji_pattern.sub(r'*', text)

	def replace(self, x):
		# 去除img标签,7位长空格
		removeImg = re.compile('<img.*?>| {7}|')
		# 删除超链接标签
		removeAddr = re.compile('<a.*?>|</a>')
		# 把换行的标签换为\n
		replaceLine = re.compile('<tr>|<div>|</div>|</p>')
		# 将表格制表<td>替换为\t
		replaceTD = re.compile('<td>')
		# 把段落开头换为\n加空两格
		replacePara = re.compile('<p.*?>')
		# 将换行符或双换行符替换为\n
		replaceBR = re.compile('<br><br>|<br>')
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		# 将&#x27;替换成'
		replacex27 = re.compile('&#x27;')
		# 将&gt;替换成>
		replacegt = re.compile('&gt;|&gt')
		# 将&lt;替换成<
		replacelt = re.compile('&lt;|&lt')
		# 将&nbsp换成''
		replacenbsp = re.compile('&nbsp;')
		# 将&#177;换成±
		replace177 = re.compile('&#177;')
		replace1 = re.compile(' {2,}')
		x = re.sub(removeImg, "", x)
		x = re.sub(removeAddr, "", x)
		x = re.sub(replaceLine, "\n", x)
		x = re.sub(replaceTD, "\t", x)
		x = re.sub(replacePara, "", x)
		x = re.sub(replaceBR, "\n", x)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub(replacex27, '\'', x)
		x = re.sub(replacegt, '>', x)
		x = re.sub(replacelt, '<', x)
		x = re.sub(replacenbsp, '', x)
		x = re.sub(replace177, u'±', x)
		x = re.sub(replace1, '', x)
		x = re.sub('\n', '', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HI18001I69T86X6D"
		proxyPass = "D74721661025B57D"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies
	
	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def get_comments(self, product_url, product_number, plat_number):  # 获取某个作品的所有评论
		pagenums = self.get_comments_total(product_url)
		print '总页数:',pagenums
		if pagenums is None:
			return None
		ss = []
		for page in range(1, pagenums + 1):
			ss.append([product_url, product_number, plat_number, page])
		pool = Pool(2)
		items = pool.map(self.get_comments_page, ss)
		# print 'items:',items
		pool.close()
		pool.join()
		mm = []
		for item in items:
			if item is not None:
				mm.extend(item)
		with open('new_data_comments.csv', 'a') as f:
			writer = csv.writer(f, lineterminator='\n')
			writer.writerows(mm)
	
	def get_comments_total(self, product_url):  # 获取某个作品评论的总页数
		p = re.compile('cn/(\d+?)/')
		book_id = re.findall(p, product_url)[0]
		url = 'https://community.321mh.com/comment/count?ssid=%s&AppId=11&commentType=2' % book_id
		retry = 5
		while 1:
			try:
				text = requests.get(url, proxies=self.GetProxies(), timeout=10).json()
				time.sleep(0.5)
				comments_num = str(text['data']).split('.')[0]
				comments_num = int(comments_num)
				if comments_num % 10 == 0:
					pagenums = comments_num / 10
				else:
					pagenums = comments_num / 10 + 1
				return int(pagenums)
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue
	
	def get_comments_page(self, ss):  # 获取某个作品每一页的评论
		product_url, product_number, plat_number, page = ss
		print '爬取页数:',page
		p0 = re.compile('cn/(\d+)')
		ssid = re.findall(p0, product_url)[0]
		print 'ssid:',ssid
		url = "https://community.321mh.com/comment/gets"
		querystring = {"ssid": ssid, "ssidType": "0", "sortType": "1", "commentType": "2", "page": str(page),
		               "pagesize": "10", "AppId": "11"}
		retry = 10
		while 1:
			try:
				user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
							   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
							   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
							   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
							   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
							   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
							   'Opera/9.52 (Windows NT 5.0; U; en)',
							   'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
							   'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
							   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
							   'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
				user_agent = random.choice(user_agents)
				headers = {
					'host': "community.321mh.com",
					'connection': "keep-alive",
					'user-agent': user_agent,
					'content-type': "application/json",
					'accept': "*/*",
					'referer': "https://www.zymk.cn/1008/",
					'accept-encoding': "gzip, deflate, br",
					'accept-language': "zh-CN,zh;q=0.9"
				}
				text = requests.get(url, headers=headers, data=querystring,  proxies=self.GetProxies(), timeout=10).json()
				# print text,'============'
				time.sleep(0.5)
				items = text['data']
				last_modify_date = self.p_time(time.time())
				results = []
				for item in items:
					nick_name = str(item['useridentifier'])
					cmt_time = self.p_time(str(item['createtime']))
					cmt_date = cmt_time.split()[0]
					if cmt_date < self.date:
						continue
					comments = item['content']
					comments = self.replace(comments)
					comments = self.remove_emoji(comments)
					like_cnt = str(item['supportcount'])
					cmt_reply_cnt = '0'
					long_comment = '0'
					src_url = product_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, src_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				if len(results) == 0:
					return None
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return None
				else:
					continue


if __name__ == "__main__":
	spider = Spider()
	# spider.get_comments('https://www.zymk.cn/146/', 'D0000012', 'P15')
	ss = []
	with open('new_data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				ss.append([i[2], i[0], 'P15'])
	for s in ss:
		print s[1],s[0]
		spider.get_comments(s[0], s[1], s[2])
