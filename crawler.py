#!/usr/bin/env python3
import sys
from urllib.parse import urlparse, urlunsplit, urlsplit, urljoin, urldefrag
from urllib import request, error
from html.parser import HTMLParser
import queue
import threading
from collections import namedtuple

DEFAULT_URL = 'yplanapp.com/'
HEADER_USER_AGENT = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'}
THREADS_NUM = 50

Task = namedtuple('Task', 'url collect_links')
ResponseResult = namedtuple('ResponseResult', 'url status error links redirect')


class LinkParser(HTMLParser):
    """
    parse html page and get all a href links
    """
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href':
                    self.links.append(value)

    def get_links(self, page):
        self.links = []
        self.feed(page)
        return self.links


def restore_relative_url(relative_url, root_url):
    """
    :param relative_url: string
    :param root_url: string
    :return: string
    join root ulr and relative path
    """
    return urljoin(root_url, relative_url)


def clean_fragments(url):
    """
    :param url: string
    :return: string
    remove anchors
    """
    return urldefrag(url).url


def is_relative(relative_url):
    """
    :param relative_url: string
    :return: bool
    check if url is relative, i.e doe snot contain domain
    """

    return not bool(urlparse(relative_url).netloc)


def is_same_domain(url, root_url):
    """
    :param url: string
    :param root_url: string
    :return: bool
    checks if url haas sa same domain as root
    """
    url = urlparse(url)
    root_url = urlparse(root_url)
    return bool(url.netloc and url.netloc == root_url.netloc)


def process_page(url, collect_links):
    """
    :param url: string
    :param collect_links: bool
    :return: ResponseResult
    fetch page by url and collect links if required
    """
    req = request.Request(url, headers=HEADER_USER_AGENT)
    try:
        with request.urlopen(req) as response:
            links = []
            if response.status < 400 and collect_links:
                # process only successful response
                page = response.read()
                charset = response.headers.get_content_charset()
                if not charset:
                    charset = 'utf-8'
                # convert bytes to string
                page = page.decode(charset)

                # parse page and fetch links
                link_parser = LinkParser()
                links = link_parser.get_links(page)

            return ResponseResult(url=url, status=response.status, error='', links=links, redirect=response.geturl())

    except error.HTTPError as e:
        error_mes = 'Server error: {}'.format(e.reason)
        return ResponseResult(url=url, status=None, error=error_mes, links=[], redirect='')
    except error.URLError as e:
        error_mes = 'Network error: {}'.format(e.reason)
        return ResponseResult(url=url, status=None, error=error_mes, links=[], redirect='')


def get_base_url(base_url_string):
    """
    :param base_url_string: string
    :return: str
    Fix url without schema and remove anchors
    aa.com/1/2/3.html?p1=1&p2=2#tag -> http://aa.com/1/2/3.html?p1=1&p2=2
    """
    if not(base_url_string.startswith('http://') or base_url_string.startswith('https://')):
        # if url is incomplete, like 'abc.com' then add basic protocol 'http://'
        base_url_string = 'http://' + base_url_string

    # remove anchors and join path it back
    return urlunsplit(urlsplit(base_url_string)[:3] + ('', ''))


def get_root_url(base_url_string):
    """
    :param base_url_string: string
    :return: string
    remove sub-domains and queries
    http://a.b.com/1/2/p.html -> http://b.com/
    """
    base_url = urlparse(base_url_string)
    # remove sub-domains from domain: aa.bb.com-> bb.com
    domain = '.'.join(base_url.netloc.split('.')[-2:])
    # use only schema + domain from original url
    root_url = urlunsplit((base_url[0], domain, '', '', ''))
    return root_url


class ThreadUrl(threading.Thread):
    def __init__(self, tasks, results):
        threading.Thread.__init__(self)
        self.tasks = tasks
        self.results = results

    def run(self):
        while True:
            task = self.tasks.get()
            if task is None:
                # got stop signal
                break
            self.results.put(process_page(task.url, task.collect_links))
            self.tasks.task_done()


def print_results(processed_urls):
    """
    :param processed_urls: dict of url->dict with status and error
    :return: None
    just print the result if got error or page status has bad code
    """
    print('Bad or broken links:')
    for url, data in processed_urls.items():
        if not data:
            continue
        if data['error'] or data['status'] > 400:
            print('"{}" status:{} error:{}'.format(url, data['status'], data['error']))


def main(base_url_string):
    """
    :param base_url_string: string
    :return: int
    main function
    """
    # url to starts with
    base_url = get_base_url(base_url_string)
    if not base_url:
        print('Incorrect url - {}'.format(base_url_string))
        return 1

    # root url so to check if link is internal or external
    root_url = get_root_url(base_url)

    # results - url and it's status
    processed_urls = {}

    # queues to exchange data with threads
    task_queue = queue.Queue()
    result_queue = queue.Queue()

    # all IO is performed by threads
    # it's faster than process links one by one
    for i in range(THREADS_NUM):
        worker = ThreadUrl(task_queue, result_queue)
        worker.setDaemon(True)
        worker.start()

    # put hte first url
    task_queue.put(Task(url=base_url, collect_links=True))

    # and wait to be completed
    task_queue.join()

    has_tasks = True

    while has_tasks:
        # main cycle
        # get results, process new links and send them back
        # work until new links are added
        has_tasks = False
        num = 0
        while not result_queue.empty():
            result = result_queue.get()
            processed_urls[result.url] = {'status': result.status, 'error': result.error}
            if result.redirect:
                processed_urls[result.redirect] = None
            processed_urls[result.url] = {'status': result.status, 'error': result.error}

            for link in result.links:
                link = clean_fragments(link)
                if not link or link.startswith(('javascript:', 'mailto:', 'whatsapp:')):
                    continue
                if is_relative(link):
                    if result.redirect:
                        link = restore_relative_url(link, result.url)
                    else:
                        link = restore_relative_url(link, result.redirect)

                if link not in processed_urls:
                    processed_urls[link] = None
                    has_tasks = True
                    num += 1

                    task = Task(url=link, collect_links=is_same_domain(link, root_url))
                    task_queue.put(task)

        task_queue.join()

    # send stop signals to all threads
    for i in range(THREADS_NUM):
        task_queue.put(None)

    print_results(processed_urls)
    return 0


if __name__ == "__main__":
    """
    Main goal:
    - find and follow all internal links
    - for external links check status only
    - check only links to web pages, skip links to resources like css, javascript, images
    USAGE:
        crawler.py starting_url
    """

    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    else:
        base_url = DEFAULT_URL

    exit(main(base_url))
