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
import os
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from logparser.Drain import LogParser
from logparser.utils import evaluator


input_dir = os.path.join(PROJECT_ROOT, "data/loghub_2k/")
output_dir = os.path.join(SCRIPT_DIR, "Drain_result/")


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

def template_set_stats(pred_set, gt_set):
    # Step 1: compute intersections and sizes
    inter_size = len(pred_set & gt_set)
    pred_size = len(pred_set)
    gt_size = len(gt_set)
    precision = inter_size / pred_size if pred_size else 0.0
    recall = inter_size / gt_size if gt_size else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return inter_size, pred_size, gt_size, precision, recall, f1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train_ratio", type=float, default=1.0)
    parser.add_argument("--freeze_templates", action="store_true", default=False)
    args = parser.parse_args()

    bechmark_result = []
    for dataset, setting in benchmark_settings.items():
        print("\n=== Evaluation on %s ===" % dataset)
        indir = os.path.join(input_dir, os.path.dirname(setting["log_file"]))
        log_file = os.path.basename(setting["log_file"])

        parser_obj = LogParser(
            log_format=setting["log_format"],
            indir=indir,
            outdir=output_dir,
            rex=setting["regex"],
            depth=setting["depth"],
            st=setting["st"],
        )

        if (not args.freeze_templates) or args.train_ratio >= 1.0:
            print("Train ratio: 1.0, freeze: False")
            parser_obj.parse(log_file)
            F1_measure, accuracy = evaluator.evaluate(
                groundtruth=os.path.join(indir, log_file + "_structured.csv"),
                parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),
            )
            bechmark_result.append([dataset, F1_measure, accuracy])
            continue

        # Two-stage incremental evaluation
        split = int(2000 * args.train_ratio)
        print(f"Train ratio: {args.train_ratio}, freeze: True, split: {split}")

        # Stage A: train on prefix
        train_result = parser_obj.parse(
            log_file,
            update=True,
            max_lines=split,
            result_name=log_file + "_train",
        )
        stage_a_templates = len(train_result["log_clusters"])

        # Stage A evaluation on subset
        gt_full_path = os.path.join(indir, log_file + "_structured.csv")
        df_gt_full = pd.read_csv(gt_full_path)
        df_gt_train = df_gt_full.iloc[:split].copy()
        train_gt_path = os.path.join(output_dir, log_file + "_train_groundtruth.csv")
        df_gt_train.to_csv(train_gt_path, index=False)
        eval_a = evaluator.evaluate(
            groundtruth=train_gt_path, parsedresult=train_result["result_path"]
        )

        # Template set comparison for stage A
        pred_set = set(pd.read_csv(train_result["result_path"])["EventTemplate"].unique())
        gt_set = set(df_gt_train["EventTemplate"].unique())
        inter_size, pred_size, gt_size, p_temp, r_temp, f1_temp = template_set_stats(
            pred_set, gt_set
        )
        print(
            "Stage A template sets - pred: {}, gt: {}, intersection: {}, precision: {:.4f}, recall: {:.4f}, f1: {:.4f}".format(
                pred_size, gt_size, inter_size, p_temp, r_temp, f1_temp
            )
        )

        # Stage B: frozen matching on suffix
        pre_templates = len(train_result["log_clusters"])
        frozen_result = parser_obj.parse(
            log_file,
            update=False,
            rootNode=train_result["rootNode"],
            logCluL=train_result["log_clusters"],
            start_from=split,
            result_name=log_file + "_frozen",
        )
        post_templates = len(frozen_result["log_clusters"])
        print(
            f"Stage A templates: {stage_a_templates}, Stage B templates after freeze: {post_templates}"
        )
        assert pre_templates == post_templates, "Template count changed during frozen matching"

        # Merge predictions for full evaluation
        df_pred_train = pd.read_csv(train_result["result_path"])
        df_pred_frozen = pd.read_csv(frozen_result["result_path"])
        df_full_pred = pd.concat([df_pred_train, df_pred_frozen], ignore_index=True)
        df_full_pred.sort_values("LineId", inplace=True)
        final_structured = os.path.join(output_dir, log_file + "_structured.csv")
        df_full_pred.to_csv(final_structured, index=False)

        eval_b = evaluator.evaluate(groundtruth=gt_full_path, parsedresult=final_structured)

        print(
            "Summary -> train_ratio: {:.2f}, freeze: {}, StageA metrics: {}, Template compare A: (inter={}, pred={}, gt={}, p={:.4f}, r={:.4f}, f1={:.4f}), StageB metrics: {}".format(
                args.train_ratio,
                True,
                eval_a,
                inter_size,
                pred_size,
                gt_size,
                p_temp,
                r_temp,
                f1_temp,
                eval_b,
            )
        )

        bechmark_result.append([dataset, eval_b[0], eval_b[1]])

    print("\n=== Overall evaluation results ===")
    df_result = pd.DataFrame(bechmark_result, columns=["Dataset", "F1_measure", "Accuracy"])
    df_result.set_index("Dataset", inplace=True)
    print(df_result)
    df_result.to_csv("Drain_bechmark_result.csv", float_format="%.6f")


if __name__ == "__main__":
    main()
