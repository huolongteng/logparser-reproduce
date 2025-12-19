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

def build_arg_parser():
    parser = argparse.ArgumentParser(description="Drain benchmark")
    parser.add_argument("--train_ratio", type=float, default=1.0, help="训练占比")
    parser.add_argument(
        "--freeze_templates", action="store_true", help="冻结模板后再匹配"
    )
    return parser


def collect_template_set(df_gt):
    # 提取模板集合用于集合对比
    return set(df_gt["EventTemplate"].unique())


def main():
    args = build_arg_parser().parse_args()
    train_ratio = args.train_ratio
    freeze_templates = args.freeze_templates
    bechmark_result = []

    print(
        f"\n>>> Args: train_ratio={train_ratio}, freeze_templates={freeze_templates}"
    )

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

        if (not freeze_templates) or train_ratio >= 1.0:
            # 保持原有全量动态评估
            parser.parse(log_file)
            F1_measure, accuracy = evaluator.evaluate(
                groundtruth=os.path.join(indir, log_file + "_structured.csv"),
                parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),
            )
            bechmark_result.append(
                {
                    "Dataset": dataset,
                    "F1_measure": F1_measure,
                    "Accuracy": accuracy,
                    "Train_F1": None,
                    "Train_Accuracy": None,
                    "Train_Templates": None,
                    "Frozen_Templates": None,
                    "Template_Intersection": None,
                }
            )
            continue

        # 解析全量数据并按顺序切分
        headers, regex = parser.generate_logformat_regex(setting["log_format"])
        df_full = parser.log_to_dataframe(
            os.path.join(indir, log_file), regex, headers, setting["log_format"]
        )
        split = int(len(df_full) * train_ratio)
        df_train = df_full.iloc[:split].copy()
        df_test = df_full.iloc[split:].copy()

        print(f"Split at {split} / {len(df_full)}")

        # 阶段A：学习模板
        parser.parse(log_file, df_subset=df_train, update=True, save_result=False)
        train_df_pred = parser.df_log.copy()
        # 同时记录聚类总数与模板集合规模，便于冻结校验
        train_templates = set(" ".join(c.logTemplate) for c in parser.logCluL)
        template_count_before = len(train_templates)
        cluster_count_before = len(parser.logCluL)

        # 阶段A评估及模板集合对比
        train_pred_path = os.path.join(output_dir, log_file + "_structured_train.csv")
        train_gt_path = os.path.join(output_dir, log_file + "_structured_train_gt.csv")
        df_gt_full = pd.read_csv(os.path.join(indir, log_file + "_structured.csv"))
        df_gt_train = df_gt_full.iloc[:split].copy()
        train_df_pred.to_csv(train_pred_path, index=False)
        df_gt_train.to_csv(train_gt_path, index=False)
        train_f1, train_acc = evaluator.evaluate(
            groundtruth=train_gt_path, parsedresult=train_pred_path
        )
        train_gt_templates = collect_template_set(df_gt_train)
        template_intersection = len(train_templates.intersection(train_gt_templates))

        print(
            f"Stage A templates learned: {template_count_before} (clusters: {cluster_count_before}), "
            f"GT templates: {len(train_gt_templates)}, Intersection: {template_intersection}"
        )

        # 阶段B：冻结匹配
        parser.parse(
            log_file,
            df_subset=df_test,
            rootNode=parser.rootNode,
            logCluL=parser.logCluL,
            update=False,
            save_result=False,
        )
        test_df_pred = parser.df_log.copy()
        cluster_count_after = len(parser.logCluL)
        assert (
            cluster_count_before == cluster_count_after
        ), "Template count changed during freeze matching"

        # 合并预测并进行全量评估
        full_pred = pd.concat([train_df_pred, test_df_pred], axis=0)
        full_pred.sort_values("LineId", inplace=True)
        final_pred_path = os.path.join(output_dir, log_file + "_structured.csv")
        full_pred.to_csv(final_pred_path, index=False)

        F1_measure, accuracy = evaluator.evaluate(
            groundtruth=os.path.join(indir, log_file + "_structured.csv"),
            parsedresult=final_pred_path,
        )

        print(
            f"Stage B matched with frozen templates. Templates before/after: {cluster_count_before}/{cluster_count_after}"
        )
        print(
            f"Train ratio: {train_ratio}, Freeze: {freeze_templates}, Train metrics: F1={train_f1:.4f}, Acc={train_acc:.4f}"
        )
        print(f"Full metrics: F1={F1_measure:.4f}, Acc={accuracy:.4f}")

        bechmark_result.append(
            {
                "Dataset": dataset,
                "F1_measure": F1_measure,
                "Accuracy": accuracy,
                "Train_F1": train_f1,
                "Train_Accuracy": train_acc,
                "Train_Templates": cluster_count_before,
                "Frozen_Templates": cluster_count_after,
                "Template_Intersection": template_intersection,
            }
        )

    print("\n=== Overall evaluation results ===")
    df_result = pd.DataFrame(
        bechmark_result,
        columns=[
            "Dataset",
            "F1_measure",
            "Accuracy",
            "Train_F1",
            "Train_Accuracy",
            "Train_Templates",
            "Frozen_Templates",
            "Template_Intersection",
        ],
    )
    df_result.set_index("Dataset", inplace=True)
    print(df_result)
    df_result.to_csv("Drain_bechmark_result.csv", float_format="%.6f")


if __name__ == "__main__":
    main()
