#!/usr/bin/env python
# -*- coding: utf-8 -*-
import appsinstalled_pb2
import memcache
import os
import gzip
import optparse
import unittest
import memc_load as mcl


opts = {'dry': False,
        'pattern': 'tests/data/tmp_uniq_keys.gz',
        'dvid': '127.0.0.1:33016',
        'adid': '127.0.0.1:33015',
        'gaid': '127.0.0.1:33014',
        'idfa': '127.0.0.1:33013'}

test_mc_cfg = {"MEMCACHE_SOCKET_TIMEOUT": 1,
               "MEMCACHE_RETRY": 0,
               "MEMCACHE_RETRY_TIMEOUT": 0}


class TestMemcacheOK(unittest.TestCase):
    expected_errors = 0
    expected_processed = 299882

    def check_integrity(self, path):
        with gzip.open(path, 'rt', encoding='utf-8') as fd:
            for line in fd:
                dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
                apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
                lat, lon = float(lat), float(lon)
                ua = appsinstalled_pb2.UserApps()
                ua.lat = lat
                ua.lon = lon
                ua.apps.extend(apps)
                expected_result = ua.SerializeToString()
                memc = self.device_memc[dev_type]
                key = "%s:%s" % (dev_type, dev_id)
                result = memc.get(key)
                self.assertEqual(result, expected_result, key)

    def setUp(self):
        os.system("killall memcached")
        os.system("memcached -p 33013& \
                   memcached -p 33014& \
                   memcached -p 33015& \
                   memcached -p 33016&")
        self.device_memc = {
            "idfa": memcache.Client(["127.0.0.1:33013"]),
            "gaid": memcache.Client(["127.0.0.1:33014"]),
            "adid": memcache.Client(["127.0.0.1:33015"]),
            "dvid": memcache.Client(["127.0.0.1:33016"]),
        }

    def tearDown(self):
        os.system("killall memcached")

    def test_ok_default_settings(self):
        mcl.CONVERT_FRAME_SIZE = 1024
        mcl.UPLOAD_FRAME_SIZE = 1024
        mcl.CONVERT_QUEUE_MAXSIZE = 32
        mcl.UPLOAD_QUEUE_MAXSIZE = 64
        mcl.UPLOADERS = 1
        reader = mcl.Reader(optparse.Values(opts), test_mc_cfg)
        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors)
        self.assertEqual(processed, self.expected_processed)
        self.check_integrity(opts['pattern'])

        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors)
        self.assertEqual(processed, self.expected_processed)

        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors)
        self.assertEqual(processed, self.expected_processed)

    def test_ok_4_loaders(self):
        mcl.CONVERT_FRAME_SIZE = 1024
        mcl.UPLOAD_FRAME_SIZE = 1024
        mcl.CONVERT_QUEUE_MAXSIZE = 32
        mcl.UPLOAD_QUEUE_MAXSIZE = 64
        mcl.UPLOADERS = 4
        reader = mcl.Reader(optparse.Values(opts), test_mc_cfg)
        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors, processed)
        self.assertEqual(processed, self.expected_processed, errors)
        self.check_integrity(opts['pattern'])

    def test_ok_by_one(self):
        mcl.CONVERT_FRAME_SIZE = 1
        mcl.UPLOAD_FRAME_SIZE = 1
        mcl.CONVERT_QUEUE_MAXSIZE = 32
        mcl.UPLOAD_QUEUE_MAXSIZE = 64
        mcl.UPLOADERS = 1
        reader = mcl.Reader(optparse.Values(opts), test_mc_cfg)
        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors, processed)
        self.assertEqual(processed, self.expected_processed, errors)
        self.check_integrity(opts['pattern'])


class TestMemcacheNotOK(unittest.TestCase):
    expected_errors = 75324
    expected_processed = 224558

    def setUp(self):
        os.system("killall memcached")
        os.system("memcached -p 33014& \
                   memcached -p 33015& \
                   memcached -p 33016&")

    def tearDown(self):
        os.system("killall memcached")

    def test_errors_count_default_settings(self):
        mcl.CONVERT_FRAME_SIZE = 1024
        mcl.UPLOAD_FRAME_SIZE = 1024
        mcl.CONVERT_QUEUE_MAXSIZE = 32
        mcl.UPLOAD_QUEUE_MAXSIZE = 64
        mcl.UPLOADERS = 1
        reader = mcl.Reader(optparse.Values(opts), test_mc_cfg)
        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors, processed)
        self.assertEqual(processed, self.expected_processed, errors)

    def test_errors_count_4_loaders(self):
        mcl.CONVERT_FRAME_SIZE = 1024
        mcl.UPLOAD_FRAME_SIZE = 1024
        mcl.CONVERT_QUEUE_MAXSIZE = 32
        mcl.UPLOAD_QUEUE_MAXSIZE = 64
        mcl.UPLOADERS = 4
        reader = mcl.Reader(optparse.Values(opts), test_mc_cfg)
        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors, processed)
        self.assertEqual(processed, self.expected_processed, errors)

    def test_errors_count_by_one(self):
        mcl.CONVERT_FRAME_SIZE = 1
        mcl.UPLOAD_FRAME_SIZE = 1
        mcl.CONVERT_QUEUE_MAXSIZE = 32
        mcl.UPLOAD_QUEUE_MAXSIZE = 64
        mcl.UPLOADERS = 1
        reader = mcl.Reader(optparse.Values(opts), test_mc_cfg)
        errors, processed = reader.file_process(opts['pattern'])
        self.assertEqual(errors, self.expected_errors, processed)
        self.assertEqual(processed, self.expected_processed, errors)
