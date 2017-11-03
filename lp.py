# coding=utf-8

import random
import urllib
import time
import math
from sklearn.metrics import roc_auc_score
import json
import matplotlib.pyplot as plt
import numpy as np



# LP spam
def lp_spam(in_file_query, in_file_url, seed_file, seed_num, out_folder):
    st = time.clock()

    # read query-url sorted by query
    print('Reading ' + in_file_query)
    fin = file(in_file_query, 'r')
    cnt = 0
    q_se = [0]
    q_ui = []
    q_ui_size = 0
    last_qi = -1
    while True:
        cnt += 1
        if cnt % 500000 == 0:
            print('  Reading query index: %dw' % (cnt / 10000))
        l = fin.readline().strip()
        if len(l) == 0:
            break
        w = l.split(' ')
        qi = int(w[0])
        ui = int(w[1])
        v = float(w[2])
        if qi > last_qi:
            last_qi = qi
            if qi > 0:
                q_se.append(q_ui_size)
        q_ui.extend([ui, v])
        q_ui_size += 2
    q_se.append(q_ui_size)
    fin.close()
    print('Use time: %f\n' % (time.clock() - st))
    st = time.clock()

    # read query-url sorted by url
    print('Reading ' + in_file_url)
    fin = file(in_file_url, 'r')
    cnt = 0
    u_se = [0]
    u_qi = []
    u_qi_size = 0
    last_ui = -1
    while True:
        cnt += 1
        if cnt % 500000 == 0:
            print('  Reading url index: %dw' % (cnt / 10000))
        l = fin.readline().strip()
        if len(l) == 0:
            break
        w = l.split(' ')
        qi = int(w[0])
        ui = int(w[1])
        v = float(w[2])
        if ui > last_ui:
            last_ui = ui
            if ui > 0:
                u_se.append(u_qi_size)
        u_qi.extend([qi, v])
        u_qi_size += 2
    u_se.append(u_qi_size)
    fin.close()
    print('Use time: %f\n' % (time.clock() - st))
    st = time.clock()

    # LP algorithm
    ql = len(q_se) - 1
    ul = len(u_se) - 1
    qva = [-1.0] * ql
    uva = [-1.0] * ul
    round_cnt = 0

    # read spam url seed
    print('Reading ' + seed_file)
    spam_ua = []
    spam_ud = {}
    spam_cnt = 0
    fin = file(seed_file, 'r')
    for l in fin.readlines():
        spam_u = int(l.strip().split(' ')[0])
        if spam_u not in spam_ud:
            spam_ua.append(spam_u)
            spam_ud[spam_u] = 1
            spam_cnt += 1
        if spam_cnt >= seed_num:
            break
    fin.close()
    print('Seed URL num: %d\n' % len(spam_ua))

    # Main
    while True:

        round_cnt += 1

        # label reset
        for su in spam_ua:
            uva[su] = 1.0

        # url to query
        print('Propagate from URL to Query')
        for u in range(ul):
            if u % 500000 == 0:
                print('  U->Q s=%d r=%d i=%dw/%dw' % (seed_num, round_cnt, u / 10000, ul / 10000))
            if uva[u] < 0.0:
                continue
            for i in range(u_se[u], u_se[u + 1], 2):
                q = u_qi[i]
                # query has computed
                if qva[q] > -1.0:
                    continue
                # compute query value
                value = 0.0
                weight = 0.0
                for j in range(q_se[q], q_se[q + 1], 2):
                    u2 = q_ui[j]
                    v2 = q_ui[j + 1]
                    weight += v2
                    if uva[u2] > -1.0 and (u_se[u2 + 1] - u_se[u2] > 2 or u2 in spam_ud):
                        value += uva[u2] * v2
                value_avg = value / weight
                qva[q] = value_avg
        uva = [-1.0] * ul
        print('Use time: %f\n' % (time.clock() - st))
        st = time.clock()

        # query to url
        print('Propagate from Query to Url')
        for q in range(ql):
            if q % 500000 == 0:
                print('  Q->U s=%d r=%d i=%dw/%dw' % (seed_num, round_cnt, q / 10000, ql / 10000))
            if qva[q] < 0.0:
                continue
            for i in range(q_se[q], q_se[q + 1], 2):
                u = q_ui[i]
                # url has computed
                if uva[u] > -1.0:
                    continue
                # compute url value
                value = 0.0
                weight = 0.0
                for j in range(u_se[u], u_se[u + 1], 2):
                    q2 = u_qi[j]
                    v2 = u_qi[j + 1]
                    weight += v2
                    if qva[q2] > -1.0 and (q_se[q2 + 1] - q_se[q2] > 2):
                        value += qva[q2] * v2
                value_avg = value / weight
                uva[u] = value_avg
        qva = [-1.0] * ql
        print('Use time: %f\n' % (time.clock() - st))
        st = time.clock()

        # print result
        if round_cnt % 5 == 0:
            print('Writing result every 5 round\n')
            # write
            out_file = '%sseed%03d_round%03d.txt' % (out_folder, seed_num, round_cnt)
            print('  Writing ' + out_file)
            fou = file(out_file, 'w')
            for u in range(ul):
                if u % 500000 == 0:
                    print('  Writing s=%d r=%d i=%dw/%dw' % (seed_num, round_cnt, u / 10000, ul / 10000))
                if uva[u] < 0.0:
                    continue
                fou.write('%.20f\t%d\n' % (uva[u], u))
            fou.close()
            print('Use time: %f\n' % (time.clock() - st))
            st = time.clock()

        if round_cnt >= 20:
            break



if __name__ == '__main__':
    # mode: 1, r, n, w
    mode = 'w'
    # seed_no: total 10 seed files which number is 0 to 9
    seed_no = 0
    for i in range(1, 11):
        # seed_num: the count of seed used in LP
        seed_num = i * 50
        # data format of 'query_url_mode_sorted_query_norm.lnk' and 'pre/query_url_mode_sorted_url_norm.lnk': 
        # [query_id]\blank[url_id]\blank[normalize_weight_considered_position_bias]
        # 
        # data format of 'seed_no_mode.txt':
        # [spam_url_id]\blank[spam_url]
        lp_spam('pre/query_url_%s_sorted_query_norm.lnk' % mode, 'pre/query_url_%s_sorted_url_norm.lnk' % mode,
                'seed_%02d_%s.txt' % (seed_no, mode), seed_num, 'result_%s/' % mode)
    





