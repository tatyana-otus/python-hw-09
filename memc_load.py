#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
import appsinstalled_pb2
import memcache
from multiprocessing import Process, Queue
import time
from threading import Thread
import configparser


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "raw_apps"])

CONVERT_FRAME_SIZE = 256
UPLOAD_FRAME_SIZE = 256

CONVERT_QUEUE_MAXSIZE = 64
UPLOAD_QUEUE_MAXSIZE = 64

UPLOADERS = 1

DEF_CONFIG = {
    "MEMCACHE_SOCKET_TIMEOUT": 1,
    "MEMCACHE_RETRY": 2,
    "MEMCACHE_RETRY_TIMEOUT": 1
}


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def parse_raw_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, raw_apps)


def get_statistics(statistic_queue, count):
    errors = processed = 0
    for i in range(count):
        e, p = statistic_queue.get()
        errors = errors + e
        processed = processed + p
    return (errors, processed)


def uploader(th_id, memc_addr, q, stat_q, mc_cfg, dry_run=False):
        logging.debug("Uploader {} started, memc_addr = {}".format(th_id, memc_addr))
        processed = errors = 0
        memc = memcache.Client([memc_addr], socket_timeout=mc_cfg["MEMCACHE_SOCKET_TIMEOUT"])
        while True:
            message = q.get()
            if message == "quit":
                stat_q.put((errors, processed))
                break
            try:
                notset_keys = []
                if dry_run:
                    logging.debug("%s - %s -> %s" % (memc_addr, key, str(packed).replace("\n", " ")))
                else:
                    attempts = mc_cfg["MEMCACHE_RETRY"]
                    while True:
                        notset_keys = memc.set_multi(message)
                        if len(notset_keys) == 0:
                            break
                        time.sleep(mc_cfg["MEMCACHE_RETRY_TIMEOUT"])
                        attempts -= 1
                        if attempts <= 0:
                            raise ConnectionError
                    processed = processed + len(message)
            except Exception as e:
                processed = processed + (len(message) - len(notset_keys))
                errors = errors + len(notset_keys)
                logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))


def converter(th_id, memc_addr, q, converter_stat_q, mc_cfg, dry_run=False):
        logging.debug("Converter {} started, memc_addr = {}".format(th_id, memc_addr))
        mc_q = Queue(maxsize=UPLOAD_QUEUE_MAXSIZE)
        loaders_stat_q = Queue(maxsize=UPLOADERS)
        loaders = []
        for idx in range(UPLOADERS):
            th = Thread(target=uploader, args=(idx, memc_addr, mc_q, loaders_stat_q, mc_cfg, dry_run))
            th.start()
            loaders.append(th)
        mc_m = {}
        while True:
            message = q.get()
            if message == "quit":
                break
            for appsinstalled in message:
                try:
                    apps = [int(a.strip()) for a in appsinstalled.raw_apps.split(",")]
                except ValueError:
                    apps = [int(a.strip()) for a in appsinstalled.raw_apps.split(",") if a.isidigit()]
                    logging.info("Not all user apps are digits: `%s`" % line)
                ua = appsinstalled_pb2.UserApps()
                ua.lat = appsinstalled.lat
                ua.lon = appsinstalled.lon
                key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
                ua.apps.extend(apps)
                packed = ua.SerializeToString()
                mc_m[key] = packed
                if len(mc_m) == UPLOAD_FRAME_SIZE:
                    mc_q.put(mc_m)
                    mc_m = {}
        if mc_m:
            mc_q.put(mc_m)
        for th in loaders:
            mc_q.put("quit")
            th.join
        converter_stat_q.put(get_statistics(loaders_stat_q, UPLOADERS))


class Reader():
    ConverterInfo = collections.namedtuple("ConverterInfod", ["memc_addr", "queue"])

    def __init__(self, options, mc_cfg):
        self.pattern = options.pattern
        self.dry = options.dry
        self.stat_q = Queue(maxsize=4)
        self.info = {"idfa": self.ConverterInfo(options.idfa, Queue(maxsize=CONVERT_QUEUE_MAXSIZE)),
                     "gaid": self.ConverterInfo(options.gaid, Queue(maxsize=CONVERT_QUEUE_MAXSIZE)),
                     "adid": self.ConverterInfo(options.adid, Queue(maxsize=CONVERT_QUEUE_MAXSIZE)),
                     "dvid": self.ConverterInfo(options.dvid, Queue(maxsize=CONVERT_QUEUE_MAXSIZE))}
        self.mc_cfg = mc_cfg

    def mc_process(self):
        for fn in glob.iglob(self.pattern):
            logging.info('Processing %s' % fn)
            errors, processed = self.file_process(fn)
            self.finalize_file_process(fn, processed, errors)

    def file_process(self, fn):
        queue_frame = {"idfa": [], "gaid": [], "adid": [], "dvid": []}
        errors = processed = 0
        count = 0
        converters = []
        for dev_id in self.info:
            th = Process(target=converter, args=(count, self.info[dev_id].memc_addr,
                                                 self.info[dev_id].queue, self.stat_q,
                                                 self.mc_cfg, self.dry))
            th.start()
            converters.append(th)
            count = count + 1
        with gzip.open(fn, 'rt', encoding='utf-8') as fd:
            for line in fd:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = parse_raw_appsinstalled(line)
                if not appsinstalled:
                    errors += 1
                    continue
                conv_info = self.info.get(appsinstalled.dev_type)
                if not conv_info:
                    errors += 1
                    logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                    continue
                queue_frame[appsinstalled.dev_type].append(appsinstalled)
                if len(queue_frame[appsinstalled.dev_type]) == CONVERT_FRAME_SIZE:
                    conv_info.queue.put(queue_frame[appsinstalled.dev_type])
                    queue_frame[appsinstalled.dev_type] = []
        for dev_id in self.info:
            if queue_frame[dev_id]:
                self.info[dev_id].queue.put(queue_frame[dev_id])
            self.info[dev_id].queue.put("quit")
        for th in converters:
            th.join()
        e, p = get_statistics(self.stat_q, 4)
        return errors + e, processed + p

    def finalize_file_process(self, fn, processed, errors):
        dot_rename(fn)
        logging.info("errors = {} processed = {}".format(errors, processed))
        if not processed:
            return
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


def get_mc_config(file_path):
    cfg = DEF_CONFIG
    if os.path.isfile(file_path):
        file_cfg = configparser.ConfigParser()
        file_cfg.read(file_path, encoding='utf-8')
        cfg["MEMCACHE_SOCKET_TIMEOUT"] = int(file_cfg.get("memcache", "MEMCACHE_SOCKET_TIMEOUT",
                                                          fallback=cfg["MEMCACHE_SOCKET_TIMEOUT"]))
        cfg["MEMCACHE_RETRY"] = int(file_cfg.get("memcache", "MEMCACHE_RETRY",
                                                 fallback=cfg["MEMCACHE_RETRY"]))
        cfg["MEMCACHE_RETRY_TIMEOUT"] = int(file_cfg.get("memcache", "MEMCACHE_RETRY_TIMEOUT",
                                                         fallback=cfg["MEMCACHE_RETRY_TIMEOUT"]))
    return cfg

if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--cfg", action="store", default="memc.cfg")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    mc_cfg = get_mc_config(opts.cfg)
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        r = Reader(opts, mc_cfg)
        r.mc_process()
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
