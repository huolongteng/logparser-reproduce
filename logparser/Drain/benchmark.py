# =========================================================================
# Copyright (C) 2016-2023 LOGPAI (https://github.com/logpai).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========================================================================


import sys
import argparse
sys.path.append("../../")
from logparser.Drain import LogParser
from logparser.utils import evaluator
import os
import pandas as pd


input_dir = "../../data/loghub_2k/"  # The input directory of log file
output_dir = "Drain_result/"  # The output directory of parsing results


benchmark_settings = {
    "HDFS": {
        "log_file": "HDFS/HDFS_2k.log",
        "log_format": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
        "regex": [r"blk_-?\d+", r"(\d+\.){3}\d+(:\d+)?"],
        "st": 0.5,
        "depth": 4,
    },
    "Hadoop": {
        "log_file": "Hadoop/Hadoop_2k.log",
        "log_format": "<Date> <Time> <Level> \[<Process>\] <Component>: <Content>",
        "regex": [r"(\d+\.){3}\d+"],
        "st": 0.5,
        "depth": 4,
    },
    "Spark": {
        "log_file": "Spark/Spark_2k.log",
        "log_format": "<Date> <Time> <Level> <Component>: <Content>",
        "regex": [r"(\d+\.){3}\d+", r"\b[KGTM]?B\b", r"([\w-]+\.){2,}[\w-]+"],
        "st": 0.5,
        "depth": 4,
    },
    "Zookeeper": {
        "log_file": "Zookeeper/Zookeeper_2k.log",
        "log_format": "<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>",
        "regex": [r"(/|)(\d+\.){3}\d+(:\d+)?"],
        "st": 0.5,
        "depth": 4,
    },
    "BGL": {
        "log_file": "BGL/BGL_2k.log",
        "log_format": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        "regex": [r"core\.\d+"],
        "st": 0.5,
        "depth": 4,
    },
    "HPC": {
        "log_file": "HPC/HPC_2k.log",
        "log_format": "<LogId> <Node> <Component> <State> <Time> <Flag> <Content>",
        "regex": [r"=\d+"],
        "st": 0.5,
        "depth": 4,
    },
    "Thunderbird": {
        "log_file": "Thunderbird/Thunderbird_2k.log",
        "log_format": "<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>",
        "regex": [r"(\d+\.){3}\d+"],
        "st": 0.5,
        "depth": 4,
    },
    "Windows": {
        "log_file": "Windows/Windows_2k.log",
        "log_format": "<Date> <Time>, <Level>                  <Component>    <Content>",
        "regex": [r"0x.*?\s"],
        "st": 0.7,
        "depth": 5,
    },
    "Linux": {
        "log_file": "Linux/Linux_2k.log",
        "log_format": "<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>",
        "regex": [r"(\d+\.){3}\d+", r"\d{2}:\d{2}:\d{2}"],
        "st": 0.39,
        "depth": 6,
    },
    "Android": {
        "log_file": "Android/Android_2k.log",
        "log_format": "<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>",
        "regex": [
            r"(/[\w-]+)+",
            r"([\w-]+\.){2,}[\w-]+",
            r"\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b",
        ],
        "st": 0.2,
        "depth": 6,
    },
    "HealthApp": {
        "log_file": "HealthApp/HealthApp_2k.log",
        "log_format": "<Time>\|<Component>\|<Pid>\|<Content>",
        "regex": [],
        "st": 0.2,
        "depth": 4,
    },
    "Apache": {
        "log_file": "Apache/Apache_2k.log",
        "log_format": "\[<Time>\] \[<Level>\] <Content>",
        "regex": [r"(\d+\.){3}\d+"],
        "st": 0.5,
        "depth": 4,
    },
    "Proxifier": {
        "log_file": "Proxifier/Proxifier_2k.log",
        "log_format": "\[<Time>\] <Program> - <Content>",
        "regex": [
            r"<\d+\ssec",
            r"([\w-]+\.)+[\w-]+(:\d+)?",
            r"\d{2}:\d{2}(:\d{2})*",
            r"[KGTM]B",
        ],
        "st": 0.6,
        "depth": 3,
    },
    "OpenSSH": {
        "log_file": "OpenSSH/OpenSSH_2k.log",
        "log_format": "<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>",
        "regex": [r"(\d+\.){3}\d+", r"([\w-]+\.){2,}[\w-]+"],
        "st": 0.6,
        "depth": 5,
    },
    "OpenStack": {
        "log_file": "OpenStack/OpenStack_2k.log",
        "log_format": "<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>",
        "regex": [r"((\d+\.){3}\d+,?)+", r"/.+?\s", r"\d+"],
        "st": 0.5,
        "depth": 5,
    },
    "Mac": {
        "log_file": "Mac/Mac_2k.log",
        "log_format": "<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>",
        "regex": [r"([\w-]+\.){2,}[\w-]+"],
        "st": 0.7,
        "depth": 6,
    },
}


def run_dynamic(parser, log_file, indir):
    # 动态更新的原始流程
    parser.parse(log_file)
    return evaluator.evaluate(
        groundtruth=os.path.join(indir, log_file + "_structured.csv"),
        parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),
    )


def run_freeze(parser, log_file, indir, train_ratio):
    # 冻结流程：先学模板再冻结匹配
    parser.logName = log_file
    parser.load_data()
    df_all = parser.df_log.copy()
    total_size = len(df_all)
    split = int(total_size * train_ratio)

    print(f"train_ratio={train_ratio:.2f}, freeze_templates=True, split={split}/{total_size}")

    # 阶段 A：可更新训练
    if split == 0:
        train_root = None
        train_clusters = []
        train_templates = ["" for _ in range(total_size)]
        train_ids = ["" for _ in range(total_size)]
        precision_a = recall_a = f1_a = acc_a = 0.0
        template_precision = template_recall = template_f1 = 0.0
        gt_all = pd.read_csv(os.path.join(indir, log_file + "_structured.csv"))
        gt_train = gt_all.iloc[:0].copy()
        inter_templates = set()
        print("Stage A skipped because split=0")
    else:
        df_train = df_all.iloc[:split].copy()
        train_root, train_clusters, train_templates, train_ids = parser.parse(
            log_file, update=True, df_log=df_train, total_size=total_size
        )

        # A 阶段评估与模板集合对比
        gt_all = pd.read_csv(os.path.join(indir, log_file + "_structured.csv"))
        gt_train = gt_all.iloc[:split].copy()
        pred_train_ids = pd.Series(train_ids[:split])
        precision_a, recall_a, f1_a, acc_a = evaluator.get_accuracy(
            gt_train["EventId"], pred_train_ids
        )

        train_templates_set = set([t for t in train_templates[:split] if t])
        gt_templates_set = set(gt_train["EventTemplate"].tolist())
        inter_templates = train_templates_set & gt_templates_set
        template_precision = (
            len(inter_templates) / len(train_templates_set) if train_templates_set else 0.0
        )
        template_recall = (
            len(inter_templates) / len(gt_templates_set) if gt_templates_set else 0.0
        )
        template_f1 = (
            2 * template_precision * template_recall / (template_precision + template_recall)
            if (template_precision + template_recall) > 0
            else 0.0
        )

        print(
            f"Stage A templates learned: {len(train_clusters)}, GT templates: {len(gt_templates_set)}, "
            f"intersection: {len(inter_templates)}, precision={template_precision:.4f}, "
            f"recall={template_recall:.4f}, f1={template_f1:.4f}"
        )
        print(
            "Stage A parsing -> Precision: {:.4f}, Recall: {:.4f}, F1_measure: {:.4f}, Parsing_Accuracy: {:.4f}".format(
                precision_a, recall_a, f1_a, acc_a
            )
        )

    # 阶段 B：冻结匹配
    df_test = df_all.iloc[split:].copy()
    _, freeze_clusters, test_templates, test_ids = parser.parse(
        log_file,
        rootNode=train_root,
        logCluL=train_clusters,
        update=False,
        df_log=df_test,
        total_size=total_size,
    )
    print(
        f"Stage B template count (before/after): {len(train_clusters)}/{len(freeze_clusters)}"
    )
    assert len(train_clusters) == len(freeze_clusters), "模板数量在冻结匹配后发生变化"

    # 合并预测结果
    merged_templates = []
    merged_ids = []
    for idx in range(total_size):
        if idx < len(train_templates) and train_templates[idx]:
            merged_templates.append(train_templates[idx])
            merged_ids.append(train_ids[idx])
        else:
            merged_templates.append(test_templates[idx])
            merged_ids.append(test_ids[idx])

    df_pred_full = df_all.copy()
    df_pred_full["EventId"] = df_pred_full["LineId"].map(lambda i: merged_ids[i - 1])
    df_pred_full["EventTemplate"] = df_pred_full["LineId"].map(
        lambda i: merged_templates[i - 1]
    )
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    pred_path = os.path.join(output_dir, log_file + "_structured.csv")
    df_pred_full.to_csv(pred_path, index=False)

    f1_b, acc_b = evaluator.evaluate(
        groundtruth=os.path.join(indir, log_file + "_structured.csv"),
        parsedresult=pred_path,
    )

    return (
        precision_a,
        recall_a,
        f1_a,
        acc_a,
        template_precision,
        template_recall,
        template_f1,
        f1_b,
        acc_b,
        len(train_clusters),
        len(freeze_clusters),
    )


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--train_ratio", type=float, default=1.0)
    arg_parser.add_argument("--freeze_templates", action="store_true", default=False)
    cli_args = arg_parser.parse_args()

    bechmark_result = []
    for dataset, setting in benchmark_settings.items():
        print("\n=== Evaluation on %s ===" % dataset)
        indir = os.path.join(input_dir, os.path.dirname(setting["log_file"]))
        log_file = os.path.basename(setting["log_file"])

        parser = LogParser(
            log_format=setting["log_format"],
            indir=indir,
            outdir=output_dir,
            rex=setting["regex"],
            depth=setting["depth"],
            st=setting["st"],
        )

        if (not cli_args.freeze_templates) or cli_args.train_ratio >= 1.0:
            f1_measure, accuracy = run_dynamic(parser, log_file, indir)
            bechmark_result.append([dataset, f1_measure, accuracy])
        else:
            (
                precision_a,
                recall_a,
                f1_a,
                acc_a,
                template_precision,
                template_recall,
                template_f1,
                f1_b,
                acc_b,
                templates_before,
                templates_after,
            ) = run_freeze(parser, log_file, indir, cli_args.train_ratio)
            print(
                f"Summary: train_ratio={cli_args.train_ratio:.2f}, freeze_templates=True, "
                f"templates_before={templates_before}, templates_after={templates_after}, "
                f"StageA_F1={f1_a:.4f}, StageA_Acc={acc_a:.4f}, TemplateF1={template_f1:.4f}, "
                f"StageB_F1={f1_b:.4f}, StageB_Acc={acc_b:.4f}"
            )
            bechmark_result.append([dataset, f1_b, acc_b])

    print("\n=== Overall evaluation results ===")
    df_result = pd.DataFrame(bechmark_result, columns=["Dataset", "F1_measure", "Accuracy"])
    df_result.set_index("Dataset", inplace=True)
    print(df_result)
    df_result.to_csv("Drain_bechmark_result.csv", float_format="%.6f")
