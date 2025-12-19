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


import regex as re
import os
import pandas as pd
import hashlib
from datetime import datetime


class Logcluster:
    def __init__(self, logTemplate="", logIDL=None):
        self.logTemplate = logTemplate
        if logIDL is None:
            logIDL = []
        self.logIDL = logIDL


class Node:
    def __init__(self, childD=None, depth=0, digitOrtoken=None):
        if childD is None:
            childD = dict()
        self.childD = childD
        self.depth = depth
        self.digitOrtoken = digitOrtoken


class LogParser:
    def __init__(
        self,
        log_format,
        indir="./",
        outdir="./result/",
        depth=4,
        st=0.4,
        maxChild=100,
        rex=[],
        keep_para=True,
    ):
        """
        Attributes
        ----------
            rex : regular expressions used in preprocessing (step1)
            path : the input path stores the input log file name
            depth : depth of all leaf nodes
            st : similarity threshold
            maxChild : max number of children of an internal node
            logName : the name of the input file containing raw log messages
            savePath : the output path stores the file containing structured logs
        """
        self.path = indir
        self.depth = depth - 2
        self.st = st
        self.maxChild = maxChild
        self.logName = None
        self.savePath = outdir
        self.df_log = None
        self.log_format = log_format
        self.rex = rex
        self.keep_para = keep_para

    def hasNumbers(self, s):
        return any(char.isdigit() for char in s)

    def treeSearch(self, rn, seq):
        retLogClust = None

        seqLen = len(seq)
        if seqLen not in rn.childD:
            return retLogClust

        parentn = rn.childD[seqLen]

        currentDepth = 1
        for token in seq:
            if currentDepth >= self.depth or currentDepth > seqLen:
                break

            if token in parentn.childD:
                parentn = parentn.childD[token]
            elif "<*>" in parentn.childD:
                parentn = parentn.childD["<*>"]
            else:
                return retLogClust
            currentDepth += 1

        logClustL = parentn.childD

        retLogClust = self.fastMatch(logClustL, seq)

        return retLogClust

    def addSeqToPrefixTree(self, rn, logClust):
        seqLen = len(logClust.logTemplate)
        if seqLen not in rn.childD:
            firtLayerNode = Node(depth=1, digitOrtoken=seqLen)
            rn.childD[seqLen] = firtLayerNode
        else:
            firtLayerNode = rn.childD[seqLen]

        parentn = firtLayerNode

        currentDepth = 1
        for token in logClust.logTemplate:
            # Add current log cluster to the leaf node
            if currentDepth >= self.depth or currentDepth > seqLen:
                if len(parentn.childD) == 0:
                    parentn.childD = [logClust]
                else:
                    parentn.childD.append(logClust)
                break

            # If token not matched in this layer of existing tree.
            if token not in parentn.childD:
                if not self.hasNumbers(token):
                    if "<*>" in parentn.childD:
                        if len(parentn.childD) < self.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD["<*>"]
                    else:
                        if len(parentn.childD) + 1 < self.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        elif len(parentn.childD) + 1 == self.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken="<*>")
                            parentn.childD["<*>"] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD["<*>"]

                else:
                    if "<*>" not in parentn.childD:
                        newNode = Node(depth=currentDepth + 1, digitOrtoken="<*>")
                        parentn.childD["<*>"] = newNode
                        parentn = newNode
                    else:
                        parentn = parentn.childD["<*>"]

            # If the token is matched
            else:
                parentn = parentn.childD[token]

            currentDepth += 1

    # seq1 is template
    def seqDist(self, seq1, seq2):
        assert len(seq1) == len(seq2)
        simTokens = 0
        numOfPar = 0

        for token1, token2 in zip(seq1, seq2):
            if token1 == "<*>":
                numOfPar += 1
                continue
            if token1 == token2:
                simTokens += 1

        retVal = float(simTokens) / len(seq1)

        return retVal, numOfPar

    def fastMatch(self, logClustL, seq):
        retLogClust = None

        maxSim = -1
        maxNumOfPara = -1
        maxClust = None

        for logClust in logClustL:
            curSim, curNumOfPara = self.seqDist(logClust.logTemplate, seq)
            if curSim > maxSim or (curSim == maxSim and curNumOfPara > maxNumOfPara):
                maxSim = curSim
                maxNumOfPara = curNumOfPara
                maxClust = logClust

        if maxSim >= self.st:
            retLogClust = maxClust

        return retLogClust

    def getTemplate(self, seq1, seq2):
        assert len(seq1) == len(seq2)
        retVal = []

        i = 0
        for word in seq1:
            if word == seq2[i]:
                retVal.append(word)
            else:
                retVal.append("<*>")

            i += 1

        return retVal

    def outputResult(self, logClustL, log_templates=None, log_templateids=None):
        # step 1: 兜底初始化，兼容原始逻辑
        if log_templates is None or log_templateids is None:
            max_index = self.df_log["LineId"].max()
            log_templates = [0] * max_index
            log_templateids = [0] * max_index
            df_events = []
            for logClust in logClustL:
                template_str = " ".join(logClust.logTemplate)
                occurrence = len(logClust.logIDL)
                template_id = hashlib.md5(template_str.encode("utf-8")).hexdigest()[0:8]
                for logID in logClust.logIDL:
                    log_templates[logID - 1] = template_str
                    log_templateids[logID - 1] = template_id
                df_events.append([template_id, template_str, occurrence])
        else:
            # step 2: 在冻结模式下直接使用传入的匹配结果
            df_events = []

        # step 3: 写入结构化日志
        self.df_log["EventId"] = self.df_log["LineId"].map(
            lambda idx: log_templateids[idx - 1]
        )
        self.df_log["EventTemplate"] = self.df_log["LineId"].map(
            lambda idx: log_templates[idx - 1]
        )
        if self.keep_para:
            self.df_log["ParameterList"] = self.df_log.apply(
                self.get_parameter_list, axis=1
            )
        self.df_log.to_csv(
            os.path.join(self.savePath, self.logName + "_structured.csv"), index=False
        )

        occ_dict = dict(self.df_log["EventTemplate"].value_counts())
        df_event = pd.DataFrame()
        df_event["EventTemplate"] = self.df_log["EventTemplate"].unique()
        df_event["EventId"] = df_event["EventTemplate"].map(
            lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()[0:8]
        )
        df_event["Occurrences"] = df_event["EventTemplate"].map(occ_dict)
        df_event.to_csv(
            os.path.join(self.savePath, self.logName + "_templates.csv"),
            index=False,
            columns=["EventId", "EventTemplate", "Occurrences"],
        )

    def printTree(self, node, dep):
        pStr = ""
        for i in range(dep):
            pStr += "\t"

        if node.depth == 0:
            pStr += "Root"
        elif node.depth == 1:
            pStr += "<" + str(node.digitOrtoken) + ">"
        else:
            pStr += node.digitOrtoken

        print(pStr)

        if node.depth == self.depth:
            return 1
        for child in node.childD:
            self.printTree(node.childD[child], dep + 1)

    def parse(self, logName, rootNode=None, logCluL=None, update=True, df_log=None, total_size=None):
        print("Parsing file: " + os.path.join(self.path, logName))
        start_time = datetime.now()
        self.logName = logName
        # step 1: 复用或初始化树与聚类列表
        rootNode = rootNode if rootNode is not None else Node()
        logCluL = logCluL if logCluL is not None else []

        # step 2: 装载数据，支持外部切片
        if df_log is None:
            self.load_data()
        else:
            self.df_log = df_log

        # step 3: 预分配结果数组，按 LineId 最大值兜底
        max_line_id = self.df_log["LineId"].max()
        if pd.isna(max_line_id):
            max_line_id = 0
        total_lines = total_size if total_size is not None else len(self.df_log)
        allocate_len = max(max_line_id, total_lines)
        log_templates = ["" for _ in range(allocate_len)]
        log_templateids = ["" for _ in range(allocate_len)]

        count = 0
        for idx, line in self.df_log.iterrows():
            logID = line["LineId"]
            logmessageL = self.preprocess(line["Content"]).strip().split()
            matchCluster = self.treeSearch(rootNode, logmessageL)

            # step 4: 未匹配模板
            if matchCluster is None:
                if update:
                    newCluster = Logcluster(logTemplate=logmessageL, logIDL=[logID])
                    logCluL.append(newCluster)
                    self.addSeqToPrefixTree(rootNode, newCluster)
                    template_str = " ".join(newCluster.logTemplate)
                    template_id = hashlib.md5(template_str.encode("utf-8")).hexdigest()[0:8]
                else:
                    template_str = "<UNMATCHED>"
                    template_id = "NoMatch"
            else:
                if update:
                    newTemplate = self.getTemplate(logmessageL, matchCluster.logTemplate)
                    matchCluster.logIDL.append(logID)
                    if " ".join(newTemplate) != " ".join(matchCluster.logTemplate):
                        matchCluster.logTemplate = newTemplate
                template_str = " ".join(matchCluster.logTemplate)
                template_id = hashlib.md5(template_str.encode("utf-8")).hexdigest()[0:8]

            # step 5: 保存匹配结果（冻结模式下不改内部结构）
            log_templates[logID - 1] = template_str
            log_templateids[logID - 1] = template_id

            count += 1
            if count % 1000 == 0 or count == len(self.df_log):
                print(
                    "Processed {0:.1f}% of log lines.".format(
                        count * 100.0 / len(self.df_log)
                    )
                )

        if not os.path.exists(self.savePath):
            os.makedirs(self.savePath)

        self.outputResult(logCluL, log_templates, log_templateids)

        print("Parsing done. [Time taken: {!s}]".format(datetime.now() - start_time))
        return rootNode, logCluL, log_templates, log_templateids

    def load_data(self):
        headers, regex = self.generate_logformat_regex(self.log_format)
        self.df_log = self.log_to_dataframe(
            os.path.join(self.path, self.logName), regex, headers, self.log_format
        )

    def preprocess(self, line):
        for currentRex in self.rex:
            line = re.sub(currentRex, "<*>", line)
        return line

    def log_to_dataframe(self, log_file, regex, headers, logformat):
        """Function to transform log file to dataframe"""
        log_messages = []
        linecount = 0
        with open(log_file, "r") as fin:
            for line in fin.readlines():
                try:
                    match = regex.search(line.strip())
                    message = [match.group(header) for header in headers]
                    log_messages.append(message)
                    linecount += 1
                except Exception as e:
                    print("[Warning] Skip line: " + line)
        logdf = pd.DataFrame(log_messages, columns=headers)
        logdf.insert(0, "LineId", None)
        logdf["LineId"] = [i + 1 for i in range(linecount)]
        print("Total lines: ", len(logdf))
        return logdf

    def generate_logformat_regex(self, logformat):
        """Function to generate regular expression to split log messages"""
        headers = []
        splitters = re.split(r"(<[^<>]+>)", logformat)
        regex = ""
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(" +", "\\\s+", splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip("<").strip(">")
                regex += "(?P<%s>.*?)" % header
                headers.append(header)
        regex = re.compile("^" + regex + "$")
        return headers, regex

    def get_parameter_list(self, row):
        template_regex = re.sub(r"<.{1,5}>", "<*>", row["EventTemplate"])
        if "<*>" not in template_regex:
            return []
        template_regex = re.sub(r"([^A-Za-z0-9])", r"\\\1", template_regex)
        template_regex = re.sub(r"\\ +", r"\\s+", template_regex)
        template_regex = "^" + template_regex.replace("\<\*\>", "(.*?)") + "$"
        parameter_list = re.findall(template_regex, row["Content"])
        parameter_list = parameter_list[0] if parameter_list else ()
        parameter_list = (
            list(parameter_list)
            if isinstance(parameter_list, tuple)
            else [parameter_list]
        )
        return parameter_list
