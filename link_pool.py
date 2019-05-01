#-*-coding=utf-8-*-
''' 核心组建；所有涉及到的API都需要有返回值：0为成功，否则为失败
为提高效率，使用到了lock-free algorithm
- 所有对 self._identifier_map， self._identifier_plist 的写操作都使用了shadow copy，以避免读的时候加锁
- shadow cop工作的前提是：python的赋值操作默认都是原子的

link都是有crawls pool中各个具体的crawl instance从webpage解析中获得的
放入lookpool之后，系统会自动调度，抓取完成之后，交给下一个合适的parse进行解析
同时，每个domain的最大访问压力，也在LinkPool中考虑
TODO:
- 和CrawlUnit本地的sqlite数据库进行互交(当数据过大，避免全部存放进内存 ..)
'''

from __future__ import with_statement
from heapq import heappush, heappop

import threading
import random
import datetime
import unittest

import logging

from spifw.core.link_info import LinkInfo
logger = logging.getLogger("spifw.core.link_pool")

class LinkPool(object):
    
    LINK_PRIOR_INST = 9
    LINK_PRIOR_HOT  = 3
    LINK_PRIOR_MID  = 2
    LINK_PRIOR_SEED = 1
    LINK_PRIOR_LOW  = 1
    
    def __init__(self):
        self._identifier_map = {} # <identifier, sector_info>
        self._identifier_plist = [] # (weight, identifier_list) e.g. [(3,["a.com"]), (1,["baidu.com", "google.com"])]
        self._last_links_visit = {}  # 以identifier为key，记录下最后一个链接被sel走的时间。用以判断一个source，是否抓取已结束
        self._identifier_up_lock = threading.Lock()  # 上面三个共用的lock。一致性考虑，不能分开
        
        self._dispose_flag = False
        self._total_link = 0
    
    @property
    def PendingLinks(self):
        if self._dispose_flag:
            return -1
        total = 0
        for op_lock, heap, dedup_set, intervals, random_range, visitable_time in self._identifier_map.values():
            with op_lock:
                total = total + len(heap)
        return total
    
    @property
    def PendingLinksDist(self):
        if self._dispose_flag: return -1
        dist_map = {}
        for identifier, (op_lock, heap, dedup_set, intervals, random_range, visitable_time) in self._identifier_map.items():
            with op_lock:
                dist_map[identifier] = len(heap)
        return dist_map
    
    @property
    def LastLinksVisit(self):
        with self._identifier_up_lock:
            return dict(self._last_links_visit)
    
    def get_pending_links(self, identifier):
        '''重量级的call. 不排序，client自己去排'''
        if identifier not in self._identifier_map:
            return []
        op_lock, heap, dedup_set, intervals, random_range, visitable_time = self._identifier_map.get(identifier)
        with op_lock:
            return [(link_info.UrlString, link_info.Weight) for reverse_weight, link_info in heap]
    
    def total_pending_link(self, identifier):
        ''' 相对轻量级的call '''
        if identifier not in self._identifier_map:
            return []
        op_lock, heap, dedup_set, intervals, random_range, visitable_time = self._identifier_map.get(identifier)
        with op_lock:
            return len(heap)
    
    def dispose(self):
        if self._dispose_flag:
            raise ValueError, "Link pool is disposing !"
        self._dispose_flag = True
        with self._identifier_up_lock:
            shadow_identifier_map = self._identifier_map   # 这里是Shadow，因为已经对 ._identifier_map ._identifier_plist 进行了改变
            self._identifier_map = {}
            shadow_identifier_plist = self._identifier_plist
            self._identifier_plist = []
            self._last_links_visit = {}
        
        for identifier, sector in shadow_identifier_map.items():
            op_lock, heap, dedup_set, intervals, random_range, visitable_time = sector
            with op_lock:
                assert len(heap) == len(dedup_set)
                
                # 这不需要，因为有可能WebAgent在等待某个link的状态 
                for negative_weight, prioritized_link in heap:
                    prioritized_link.LinkStatus = LinkInfo.LINK_STATUS_ABAN
                
                sector[1] = []    # heap
                sector[2] = set() # dedup_set
                logger.warn("LINK_POOL_SOURCE_DISPOSED [idr='%s']", identifier)
        logger.warn("LINK_POOL_ALL_DISPOSED")
        return 0
    
    def declare_source(self, identifier, priority, interval_seconds, random_range = 0):
        if self._dispose_flag:
            raise ValueError, "Link pool is disposing! Cannot declare."
        if interval_seconds - random_range < 0:
            raise ValueError, "random_range cannot be greater than interval_seconds"        
        
        sector = [
            threading.Lock(),    # op lock
            [],                  # heap. format (-link_info.Weight, link_info)
            set(),               # dedup set            
            interval_seconds,    # 一个站点两次抓取之前的间隔
            random_range,        # 为了使访问更像人工所为，加入随机因素 actual_intv = [interval_seconds-random_range, interval_seconds+random_range]
            datetime.datetime.strptime('2010-01-01 12:00:00', '%Y-%m-%d %H:%M:%S')  # visitable_time 下一个可访问的时间，只有当now >= visitable_time，才可以   
        ]
        
        with self._identifier_up_lock:
            if identifier in self._identifier_map:
                raise ValueError, "Duplicated identifier: " + identifier
        
            shadow_identifier_map = dict(self._identifier_map)
            shadow_identifier_plist = list(self._identifier_plist)
            
            shadow_identifier_map[identifier] = sector              
            index = 0
            identifier_list = None
            while index < len(shadow_identifier_plist):
                current_priority, identifier_list = shadow_identifier_plist[index]
                if priority > current_priority:
                    identifier_list = None
                    break
                elif priority == current_priority:            
                    break
                index = index + 1
            if identifier_list == None:
                identifier_list = [identifier]
                shadow_identifier_plist.insert(index, (priority, identifier_list))
            else:
                identifier_list.append(identifier)
            
            # 最后再把shadow的操作，赋值回去
            self._identifier_map = shadow_identifier_map
            self._identifier_plist = shadow_identifier_plist
            
            self._last_links_visit[identifier] = None # 要在update锁里面
        
        return 0
    
    def cleanup_source(self, identifier):
        if self._dispose_flag:
            logger.warn("NEED_NOT_CLEAN_FOR_DISPOSED [idr='%s']", identifier)
            return 0
        
        with self._identifier_up_lock:
            if not identifier in self._identifier_map:
                raise ValueError, "No such identifier: " + identifier
            
            shadow_identifier_map = dict(self._identifier_map)
            shadow_identifier_plist = list(self._identifier_plist)
            
            # 对 identifier_map 的清理操作
            sector = shadow_identifier_map.pop(identifier)      
            op_lock, heap, dedup_set, intervals, random_range, visitable_time = sector
            with op_lock:
                assert len(heap) == len(dedup_set)
                
                # 这是需要的，因为有可能WebAgent在等待某个link的状态 
                for negative_weight, prioritized_link in heap:
                    prioritized_link.LinkStatus = LinkInfo.LINK_STATUS_ABAN                
                sector[1] = []    # heap
                sector[2] = set() # dedup_set
            
            # 对 identifier_plist 的清理操作
            index = 0
            while index < len(shadow_identifier_plist):
                priority, identifier_list = shadow_identifier_plist[index]                
                if identifier in identifier_list:
                    identifier_list.remove(identifier)
                    if len(identifier_list) == 0:
                        del shadow_identifier_plist[index]
                    break
                index = index + 1
            
            # 最后再把shadow的操作，赋值回去
            self._identifier_map = shadow_identifier_map
            self._identifier_plist = shadow_identifier_plist
            
            self._last_links_visit.pop(identifier) # 一定有。另外，操作要在update锁里面
            
        logger.info("LINK_SOURCE_CLEAN_UP [idr='%s']", identifier)
        return 0
    
    def put_link(self, link_info):
        ''' 返回值见 PUT_LINK_RET_XXX
        Thread-safe
        '''
        if not isinstance(link_info, LinkInfo):
            raise TypeError, "link_info should be LinkInfo"
        
        sector = self._identifier_map.get(link_info.Identifier, None) # 这里不需要Refer出来；对赋值来说，get无所谓
        if sector == None:
            logger.fatal("PUT_INVALID_IDR [idr='%s'][res='%s']", link_info.Identifier, "Do you forget to declare_source it?")
            return -1
        
        # 把需要操作的东西，一下全部拿出来
        op_lock  = sector[0]
        with op_lock:
            if self._dispose_flag:
                return -1
                # 需要在lock中做，不然会有risk
            
            heap, dedup_set, intervals, random_range, visitable_time = sector[1:]
            link_info_key = (link_info.UrlString, hash(link_info.PostData), hash(str(link_info.Headers))) 
            if link_info_key in dedup_set:
                # BUFFIX, Postdata同样需要作为key，放入dedup set
                # PostData为None时，hash也不会有问题
                return 1
            heappush(heap, (-link_info.Weight, link_info)) # 注意，这个weight是倒过来的；因为要从大到小的拿
            dedup_set.add(link_info_key)
            assert len(heap) == len(dedup_set)
            
            self._last_links_visit[link_info.Identifier] = None
            
        link_info.LinkStatus = LinkInfo.LINK_STATUS_PEND
        self._total_link = self._total_link + 1
         
        return 0
    
    
    def sel_link(self):
        ''' 挑选的过程分两个步骤
        Step 1: 根据identifier declare_source时的权重，先选出从哪个identifier sector中挑选link。规则，总是从identifier priority最高的identifier中选
        Step 2: 从目标identifier sector中，跳出权重最大linker返回
        Thread-safe
        '''
        if self._dispose_flag:
            # 这里有risk，并没有关系
            return None
        
        identifier_rand = random.Random()
        approximate_now = datetime.datetime.now()
        
        refer_identifier_plist = self._identifier_plist # 注意，这里一定需要先赋值出来，才可以在下一步的for循环中被迭代
        for priority, identifier_list in refer_identifier_plist:
            if len(identifier_list) == 1:
                temp_identifier_map = identifier_list  # 这里不是shadow
            else:
                temp_identifier_map = list(identifier_list)
                temp_identifier_map.sort(cmp = lambda e1, e2: identifier_rand.choice((-1, 1)))
            
            # BUG-FIXED 需要先遍历High Priority的source；都无效时，才选择低priority的source
            for identifier in temp_identifier_map:            
                sector = self._identifier_map.get(identifier, None) # 这里不需要先赋值出来；对赋值来说，get无所谓
                if sector == None:
                    logger.warn("UNSYNC_IDR_PLIST_MAP [idr='%s'][msg='%s']", identifier, "Might be caused by crawls' dynamic loading")
                    continue
                
                op_lock = sector[0]
                with op_lock:
                    heap, dedup_set, intervals, random_range, visitable_time = sector[1:]
                    if approximate_now < visitable_time:
                        continue
                    if len(heap) == 0:
                        continue
                    
                    negative_weight, prioritized_link = heappop(heap)
                    link_info_key = (prioritized_link.UrlString, hash(prioritized_link.PostData), hash(str(prioritized_link.Headers)))
                    dedup_set.remove(link_info_key)
                    assert len(heap) == len(dedup_set)
                    
                    delta_seconds = intervals + identifier_rand.randint(-random_range, random_range)
                    new_visitable_time = approximate_now + datetime.timedelta(0, delta_seconds)
                    sector[5] = new_visitable_time
                    
                    if len(heap) == 0: # 说明刚刚sel走的是最后一条                        
                        self._last_links_visit[identifier] = approximate_now
                    
                    return prioritized_link
        return None
    

class CaseLinkPool(unittest.TestCase):
    
    def testDomainPlist(self):
        lp = LinkPool()
        lp.declare_source("baidu.com", priority = 1, interval_seconds = 0, random_range = 0)
        lp.declare_source("dianping.com", priority = 3, interval_seconds = 0, random_range = 0)
        lp.declare_source("google.com", priority = 1, interval_seconds = 0, random_range = 0)
        lp.declare_source("aipang.com", priority = 3, interval_seconds = 0, random_range = 0)
        lp.declare_source("jimi.com", priority = 5, interval_seconds = 0, random_range = 0)
        self.assertEqual(lp._identifier_plist, [(5, ['jimi.com']), (3, ['dianping.com', 'aipang.com']), (1, ['baidu.com', 'google.com'])])
        
        aipang_firsts = 0
        dianping_firsts = 0
        for i in range(100):
            lp.put_link(LinkInfo("baidu1.html", 1, "baidu.com"))
            lp.put_link(LinkInfo("baidu2.html", 2, "baidu.com"))
            lp.put_link(LinkInfo("dianping.html", 1, "dianping.com"))
            lp.put_link(LinkInfo("aipang.html", 1, "aipang.com"))
            lp.put_link(LinkInfo("jimi.html", 1, "jimi.com"))
            url_list = []
            while lp.PendingLinks > 0:
                l = lp.sel_link()
                if l == None:
                    continue
                url_list.append(l.UrlString)
            if url_list[1] == 'aipang.html':                
                self.assertTrue(url_list, ['jimi.html', 'aipang.html', 'dianping.html', 'baidu2.html', 'baidu1.html'])
                aipang_firsts = aipang_firsts + 1
            else:
                self.assertTrue(url_list, ['jimi.html', 'dianping.html', 'aipang.html', 'baidu2.html', 'baidu1.html'])
                dianping_firsts = dianping_firsts + 1
        self.assertTrue(abs(aipang_firsts - dianping_firsts) < 50) # 概论
        
    
if __name__ == "__main__":
    unittest.main()

    
