# coding=utf-8

import time
import os


def data_init(mode):
    # weight vector
    wvd = {}
    wvd['1'] = [0.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000, 1.000]
    wvd['r'] = [0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000, 10.000, 11.000]
    wvd['n'] = [0.000, 1.000, 2.937, 5.238, 7.211, 9.936, 13.393, 17.142, 20.822, 23.814, 25.430, 83.094]
    wvd['w'] = [0.000, 1.000, 1.951, 2.340, 2.580, 2.767, 2.910, 3.020, 3.108, 3.183, 3.249, 3.313]
    wv = wvd[mode]
    # read query url link
    fin = file('query_url.lnk', 'r')
    ls = fin.readlines()
    fin.close()
    # handle
    qa = []
    qd = {}
    ua = []
    ud = {}
    res = []
    for l in ls:
        w = l.strip().split('\t')
        q = w[0]
        u = w[1]
        n = int(w[2])
        r = int(w[3])
        if r > 11:
            r = 11
        v = float(n) * wv[r]
        if q not in qd:
            qd[q] = len(qa)
            qa.append(q)
        if u not in ud:
            ud[u] = len(ua)
            ua.append(u)
        res.append([qd[q], ud[u], v])
    # write result sorted by query
    res.sort(key=lambda res: res[1])
    res.sort()
    fou = file('query_url_%s_sorted_query.lnk' % mode, 'w')
    for quv in res:
        fou.write('%d %d %.3f\n' % (quv[0], quv[1], quv[2]))
    fou.close()
    # write result sorted by query
    res.sort(key=lambda res: res[1])
    fou = file('query_url_%s_sorted_url.lnk' % mode, 'w')
    for quv in res:
        fou.write('%d %d %.3f\n' % (quv[0], quv[1], quv[2]))
    fou.close()


# LP spam
def lp_spam(in_file_query, in_file_url, seed_file, seed_num, out_folder):
    st = time.clock()
    if not os.path.exists(out_folder):
        os.makedirs(out_folder)

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
    mode_arr = ['1', 'r', 'n', 'w']
    # seed_num: the count of seed used in LP
    seed_num = 2
    # data init
    for mode in mode_arr:
        data_init(mode)
    # lp 
    for mode in mode_arr:
        lp_spam('query_url_%s_sorted_query.lnk' % mode, 'query_url_%s_sorted_url.lnk' % mode,
                'seed.txt', seed_num, 'result_%s/' % mode)






