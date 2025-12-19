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
sys.path.append("../../")
from logparser.Drain import LogParser
from logparser.utils import evaluator
import os
import argparse
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

def parse_arguments():
    parser = argparse.ArgumentParser(description="Drain benchmark runner")
    parser.add_argument("--train_ratio", type=float, default=1.0, help="比例")
    parser.add_argument(
        "--freeze_templates", action="store_true", help="冻结模板后评估"
    )
    return parser.parse_args()


def evaluate_subset(gt_path, pred_path):
    return evaluator.evaluate(groundtruth=gt_path, parsedresult=pred_path)


def build_template_report(df_pred, save_path):
    occ_dict = dict(df_pred["EventTemplate"].value_counts())
    df_event = df_pred[["EventId", "EventTemplate"]].drop_duplicates()
    df_event["Occurrences"] = df_event["EventTemplate"].map(occ_dict)
    df_event.to_csv(
        save_path,
        index=False,
        columns=["EventId", "EventTemplate", "Occurrences"],
    )


def main():
    args = parse_arguments()
    train_ratio = args.train_ratio
    freeze_templates = args.freeze_templates

    bechmark_result = []
    for dataset, setting in benchmark_settings.items():
        print("\n=== Evaluation on %s ===" % dataset)
        print(
            f"Config: train_ratio={train_ratio}, freeze_templates={freeze_templates}"
        )
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

        # 兼容：默认路径仍然动态更新
        if (not freeze_templates) or train_ratio >= 1.0:
            parser.parse(log_file)
            F1_measure, accuracy = evaluate_subset(
                gt_path=os.path.join(indir, log_file + "_structured.csv"),
                pred_path=os.path.join(output_dir, log_file + "_structured.csv"),
            )
            bechmark_result.append([dataset, F1_measure, accuracy])
            continue

        # 阶段切分
        print("进入两阶段评估模式（冻结模板）")
        headers, regex = parser.generate_logformat_regex(setting["log_format"])
        full_df = parser.log_to_dataframe(
            os.path.join(indir, log_file), regex, headers, setting["log_format"]
        )
        total_lines = len(full_df)
        split = int(total_lines * train_ratio)
        train_df = full_df.iloc[:split].copy()
        test_df = full_df.iloc[split:].copy()

        # 阶段A：学习模板
        train_log_name = log_file + "_train"
        train_root, train_clusters = parser.parse(
            train_log_name, df_log_override=train_df, update=True
        )
        train_pred_df = parser.df_log.copy()
        train_templates = {" ".join(c.logTemplate) for c in train_clusters}
        train_template_count = len(train_templates)
        print(f"阶段A模板数: {train_template_count}")

        # 阶段A：子集评估
        train_pred_path = os.path.join(output_dir, train_log_name + "_structured.csv")
        train_gt_df = pd.read_csv(os.path.join(indir, log_file + "_structured.csv"))
        train_gt_df = train_gt_df.iloc[:split].copy()
        train_gt_path = os.path.join(output_dir, log_file + "_train_gt.csv")
        train_gt_df.to_csv(train_gt_path, index=False)
        F1_train, acc_train = evaluate_subset(train_gt_path, train_pred_path)
        print(f"阶段A评估: F1={F1_train:.4f}, Accuracy={acc_train:.4f}")

        # 阶段A：模板集合对比
        gt_templates = set(train_gt_df["EventTemplate"].dropna().unique())
        inter_size = len(train_templates.intersection(gt_templates))
        precision = inter_size / train_template_count if train_template_count else 0
        recall = inter_size / len(gt_templates) if len(gt_templates) else 0
        f1_temp = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0
        )
        print(
            f"模板集合对比A: 预测={train_template_count}, GT={len(gt_templates)}, "
            f"交集={inter_size}, P={precision:.4f}, R={recall:.4f}, F1={f1_temp:.4f}"
        )

        # 阶段B：冻结匹配
        freeze_log_name = log_file + "_freeze"
        before_template_count = len(train_clusters)
        freeze_parser = LogParser(
            log_format=setting["log_format"],
            indir=indir,
            outdir=output_dir,
            rex=setting["regex"],
            depth=setting["depth"],
            st=setting["st"],
        )
        _, frozen_clusters = freeze_parser.parse(
            freeze_log_name,
            df_log_override=test_df,
            root=train_root,
            log_clusters=train_clusters,
            update=False,
        )
        after_template_count = len(frozen_clusters)
        print(
            f"阶段B模板数（应保持不变）: {after_template_count}，一致性检查: "
            f"{before_template_count == after_template_count}"
        )
        assert (
            before_template_count == after_template_count
        ), "冻结模式下模板数发生了变化"

        # 合并预测并输出最终文件
        test_pred_df = freeze_parser.df_log.copy()
        full_pred_df = pd.concat([train_pred_df, test_pred_df], ignore_index=True)
        final_structured_path = os.path.join(output_dir, log_file + "_structured.csv")
        full_pred_df.to_csv(final_structured_path, index=False)
        build_template_report(
            df_pred=full_pred_df,
            save_path=os.path.join(output_dir, log_file + "_templates.csv"),
        )

        # 阶段B：全量评估
        F1_full, acc_full = evaluate_subset(
            gt_path=os.path.join(indir, log_file + "_structured.csv"),
            pred_path=final_structured_path,
        )
        print(f"阶段B评估: F1={F1_full:.4f}, Accuracy={acc_full:.4f}")
        bechmark_result.append([dataset, F1_full, acc_full])

    print("\n=== Overall evaluation results ===")
    df_result = pd.DataFrame(
        bechmark_result, columns=["Dataset", "F1_measure", "Accuracy"]
    )
    df_result.set_index("Dataset", inplace=True)
    print(df_result)
    df_result.to_csv("Drain_bechmark_result.csv", float_format="%.6f")


if __name__ == "__main__":
    main()
