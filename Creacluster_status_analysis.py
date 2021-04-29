# -*- coding:utf-8 -*-
"""
analyse cluster status exception,
匹配常用的case, 返回explain完整结果
"""
import espaas_api
import argparse
import traceback
import json
import logging
import gevent
import time
import datetime
import requests

from libs.log import initlog
from gevent import monkey

monkey.patch_all()
timeout = 10

# 7.10 elasticsearch 有16种决策器
dict = {
    'same_shard': '默认配置下一个节点不允许分配同一shard的多个副本分片', # 不参与统一处理
    'shards_limit': '节点限制单索引shard数，调整相应索引的total_shards_per_node参数',
    'disk_threshold': '磁盘空间达到水位线无法分配shard，调整磁盘容量、节点数或者清理磁盘',
    'max_retry': '达到allocate最大重试次数，尝试调用API手动retry',
    'awareness': '副本分配过多，大于awareness的配置',
    'cluster_rebalance': '集群正在Rebalance，可忽略',
    'concurrent_rebalance': '集群当前正在Rebalance，可忽略',
    'node_version': '分配到的节点版本不一致，请联系管理员调整',
    'replica_after_primary_active': '主分片未active，等待并观察主分片',
    'filter': '未通过filter，请检查配置',
    'enable': '集群enable限制',
    'throttling': '集群正在恢复，需要按照优先级恢复 primary > replica',
    'rebalance_only_when_active': 'Only allow rebalancing when all shards are active within the shard replication group',
    'resize': 'An allocation decider that ensures we allocate the shards of a target index for resize operations next to the source primaries',
    'restore_in_progress': 'This allocation decider prevents shards that have failed to be restored from a snapshot to be allocated',
    'snapshot_in_progress': 'This allocation decider prevents shards that are currently been snapshotted to be moved to other nodes'
}


def check_exception_status_reason_advice(cluster):
    # 省略结果数据入库代码
    res = {
        "status": "",
        "reason": "",
        "advice": "",
        "detail": ""
    }
    # 1.health API
    # 2.node count check
    url = gen_url(cluster) + "/_cat/health?format=json"
    r = requests.get(url, auth=(cluster["username"], cluster["password"]), timeout=timeout)
    if r.status_code == 200:
        try:
            content = json.loads(r.content)
            cluster_status_res = content[0]
            res["status"] = cluster_status_res["status"]
            if cluster_status_res["status"] == "green":
                return res
            if cluster_status_res["node.total"] != cluster["nodes_count"]:
                res["reason"] = "node_left"
                res["advice"] = "请检查k8s环境中node数, 使用`kubectl describe po pod_name`检查crash reason"
                return res
        except:
            traceback.print_exc()
    else:
        res["status"] = "unknown"
        return res

    # 3.get UNASSIGNED shard
    get_all_shard_url = gen_url(cluster) + "_cat/shards?format=json"
    get_all_shard_res = requests.get(get_all_shard_url, auth=(cluster["username"], cluster["password"]),
                                     timeout=timeout)
    unassigned_shard_list = []
    unassigned_shard_final_list = []
    if get_all_shard_res.status_code == 200:
        try:
            shards = json.loads(get_all_shard_res.content)
            for shard in shards:
                if shard["state"] == "UNASSIGNED":
                    unassigned_shard_list.append(shard)
            if len(unassigned_shard_list) > 20:
                unassigned_shard_final_list = unassigned_shard_list[0:20]
            else:
                unassigned_shard_final_list = unassigned_shard_list
        except:
            traceback.print_exc()

    # 4.explain
    # 5.经验值提取出常用建议
    decider_list = []
    explanation_list = []
    has_advice = 0
    explain_res_all = []
    for shard_tmp in unassigned_shard_final_list:
        index_name_tmp = shard_tmp["index"]
        shard_pos = shard_tmp["shard"]
        shard_type = shard_tmp["prirep"]
        is_primary = False
        if shard_type == "p":
            is_primary = True
        explain_url = gen_url(cluster) + "/_cluster/allocation/explain"
        payload = {
            "index": index_name_tmp,
            "shard": shard_pos,
            "primary": is_primary
        }
        explain_res = requests.post(explain_url, json.dumps(payload),
                                    auth=(cluster["username"], cluster["password"]),
                                    headers={"Content-Type": "application/json"}).json()
        explain_res_all.append(explain_res)
        # 整理单个分片所有的decider信息
        same_shard_count = 0
        for decisions_tmp in explain_res["node_allocation_decisions"]:
            for decider_tmp in decisions_tmp['deciders']:
                decider_list.append(decider_tmp['decider'])
                explanation_list.append(decider_tmp['explanation'])
                if decider_tmp['decider'] == "same_shard":
                    same_shard_count = same_shard_count + 1

        tmp_string = ''
        explanation_list_string = tmp_string.join(explanation_list)
        if explanation_list_string.find("ik_max_word") != -1:
            res["reason"] = "max_retry"
            res["advice"] = "可能触发6.8.5Ik分词器bug，请使用其他兼容版本或根据issue fix"
            has_advice = 1
            break
        if explanation_list_string.find("cluster.routing.allocation.enable=primaries") == -1 \
                and explanation_list_string.find("cluster.routing.allocation.enable=replicas") == -1:
            print '正常'
        else:
            res["reason"] = "enable"
            res["advice"] = "cancel cluster.routing.allocation.enable=replicas/primaries settings"
            has_advice = 1
            break
        if 'same_shard' in decider_list and same_shard_count == cluster["nodes_count"]:
            res["reason"] = "same_shard"
            res["advice"] = "shard副本设置过多，调整副本数量"
            has_advice = 1
            break
        # 取decider与ES决策器keys的交集
        decider_and = list(set(decider_list).intersection(set(dict.keys())))
        reason_and = ''
        advice_and = ''
        if list(set(decider_list).intersection(set(dict.keys()))):
            for x in decider_and:
                if x != 'same_shard':
                    reason_and += x + ';'
                    advice_and += dict[x] + ';'
            res["reason"] = reason_and
            res["advice"] = advice_and

    if has_advice == 0:
        res["detail"] = explain_res_all
    return res


def gen_url(cluster):
    url = 'http://' + cluster["domain"] + ':' + str(cluster["http_port"]) + cluster["http_path"]
    if url.endswith("/"):
        url = url[:-1]
    return url


def main(cluster_id=None):
    try:
        if cluster_id:
            clusters = espaas_api.get("cluster", "/api/get_cluster_by?cluster_id=%s" % cluster_id)["data"]
        else:
            clusters = espaas_api.get("cluster", "/api/all_status_exception_cluster")["data"]
        jobs = []
        for cluster in clusters:
            try:
                jobs.append(gevent.spawn(check_exception_status_reason_advice, cluster))
            except:
                traceback.print_exc()
        logging.info("send job number: %s" % len(jobs))
        gevent.joinall(jobs)
    except:
        traceback.print_exc()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", default="-", help="log file")
    parser.add_argument("--level", default="info")
    args = parser.parse_args()
    initlog(level=args.level, log=args.l)
    main()

