# -*- coding: utf-8 -*-
import sys
import re
import os
import time
import Queue
import thread
import threading
import urllib
import urllib2
import numpy as np

URL_IDX = "http://verbs.colorado.edu/chinese/cpb/html_frames/"
exitFlag = 0
queueLock = threading.Lock()
workQueue = Queue.LifoQueue(20000)
threads = []
threadNum = 12

class myThread(threading.Thread):
    def __init__(self, tid, name, q):
        threading.Thread.__init__(self)
        self.tid = tid
        self.name = name
        self.q = q

    def run(self):
        print "Starting " + self.name
        process(self.name, self.q)
        print "Exiting " + self.name


def process(name, q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            data = q.get()
            queueLock.release()
            spider(name, data, q)
        else:
            queueLock.release()
        sleep = np.random.randint(5)
        time.sleep(sleep)


def spider(thread_name, data, q):
    name_url = "%s\t%s" % data
    try:
        crawl(data)
        sys.stdout.write("SUCC Thread[%s] %s\n" % (thread_name, name_url))
    except Exception as err:
        sys.stderr.write("FAIL Thread[%s] Url[%s] Error: %s\n" % (thread_name, name_url, err))
        queueLock.acquire()
        q.put(data)
        queueLock.release()


def crawl(data):
    headers = {}
    headers['Content-Type'] = 'text/html'
    name, url = data
    req = urllib2.Request(url, headers=headers)
    page = urllib2.urlopen(req)
    res = page.read()
    page.close()
    file_out = "frame/%s" % name
    with open(file_out, "w") as f:
        f.write(res)


for i in range(threadNum):
    tid = i
    tname = "TR-%s" % tid
    thread = myThread(tid, tname, workQueue)
    thread.start()
    threads.append(thread)


def get_url(filename):
    url_list = []
    with open(filename) as f:
        for line in f:
            ret = line.strip().split("\t")
            name = ret[1]
            url = URL_IDX + name
            url_list.append((name, url))
    return url_list

    
def curl():
    filename = sys.argv[1]
    url_list = get_url(filename)
    sleep = np.random.randint(5)
    count = 0
    batch = 10
    for item in url_list:
        name, url = item
        cmd = "curl -s %s > frame/%s &" % (url, name)
        ret = os.system(cmd)
        if ret != 0:
            sys.stderr.write(cmd + "\n")
        if count >= batch:
            count = 0
            time.sleep(sleep)
        else:
            count += 1


def run():
    global exitFlag
    start = time.time()
    print "Start Main Thread %s" % time.ctime()
    filename = sys.argv[1]
    url_list = get_url(filename)
    queueLock.acquire()
    for item in url_list:
        workQueue.put(item)
    queueLock.release()
    print "Queue size %s" % workQueue.qsize()

    while not workQueue.empty():
        pass
    exitFlag = 1
    for t in threads:
        t.join()
    print "Exiting Main Thread %s" % time.ctime()
    end = time.time()
    print "Time Cost %s" % (end - start)


run()

