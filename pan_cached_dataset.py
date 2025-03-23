#!/usr/bin/env python
# coding: utf-8
"""
This module provides a way to access data sets from Pangaea.
"""
import os
from abc import ABC
from enum import Enum, auto

import pandas as pd
import pangaeapy.pandataset as pgpd


class PanFileDataSetState(Enum):
    """
    Pangaea file data set state.
    """
    INIT = auto()
    DESCRIPTION = auto()
    HEADER = auto()
    DATA = auto()
    END = auto()


class PanFileDataSetDescriptionState(Enum):
    """
    Pangaea file data set description sub state.
    """
    INIT = auto()
    CITATION = auto()
    ABSTRACT = auto()
    KEYWORDS = auto()
    PARAMETERS = auto()
    SIZE = auto()
    LICENSE = auto()
    FURTHER_DETAILS = auto()
    END = auto()


class PanFileDataSet:
    """
    Pangaea data set from File
    """

    def __init__(self, file_path):
        self.file_path = file_path
        self._header = None
        self._data = None
        self.loaded = False
        self._columns = None
        self.citations = None
        self.parameters = []
        self.keywords = []
        self.size = None
        self.license = None
        self.further_details = None
        self.abstract = None

    def info(self):
        """
        Print the data set info.
        """
        self._load()
        print(f"Abstract:\n\t{self.abstract}")
        print(f"Header:\n\t{self._header}")
        print(f"Columns:\n\t{self._columns}")
        print(f"Citations:\n\t{self.citations}")
        print(f"Parameters:\n\t-{"\n\t- ".join(self.parameters)}")
        print(f"Keywords:\n\t{self.keywords}")
        print(f"Size:\n\t{self.size}")
        print(f"License:\n\t{self.license}")
        print(f"Further Details:\n\t{self.further_details}")
        print(f"Data:\n\t{self.data}")

    def _load(self):
        """
        Load the data set from the file into panda data frame
        """
        if self.loaded:
            return
        state: PanFileDataSetState = PanFileDataSetState.INIT
        description_state: PanFileDataSetDescriptionState = PanFileDataSetDescriptionState.INIT
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        skiprows = 0
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                skiprows += 1
                if line == "/* DATA DESCRIPTION:":
                    state = PanFileDataSetState.DESCRIPTION
                    continue
                elif line == "*/":
                    state = PanFileDataSetState.HEADER
                    continue
                elif state == PanFileDataSetState.DESCRIPTION:
                    fields = line.split("\t", maxsplit=1)
                    if len(fields) ==2:
                        field, value = fields
                        field = field.strip()
                        if field == "Citation:":
                            self.citations = value
                            description_state = PanFileDataSetDescriptionState.CITATION
                        elif field == "Abstract:":
                            self.abstract = value
                            description_state = PanFileDataSetDescriptionState.ABSTRACT
                        elif field == "Keyword(s):":
                            self.keywords = value.split(";")
                            description_state = PanFileDataSetDescriptionState.KEYWORDS
                        elif field == "Parameter(s):":
                            self.parameters.append(value)
                            description_state = PanFileDataSetDescriptionState.PARAMETERS
                        elif field == "Size:":
                            self.size = value
                            description_state = PanFileDataSetDescriptionState.SIZE
                        elif field == "License:":
                            self.license = value
                            description_state = PanFileDataSetDescriptionState.LICENSE
                        elif field == "Further Details:":
                            self.further_details = value
                            description_state = PanFileDataSetDescriptionState.FURTHER_DETAILS
                    else:
                        value = fields[0]
                        if description_state == PanFileDataSetDescriptionState.PARAMETERS:
                            self.parameters.append(value)
                        elif description_state == PanFileDataSetDescriptionState.KEYWORDS:
                            self.keywords = self.keywords.append(
                                value.split(";"))
                        elif description_state == PanFileDataSetDescriptionState.FURTHER_DETAILS:
                            self.further_details = self.further_details + value
                        elif description_state == PanFileDataSetDescriptionState.ABSTRACT:
                            self.abstract = self.abstract + value
                        elif description_state == PanFileDataSetDescriptionState.CITATION:
                            self.citations = self.citations + value
                        elif description_state == PanFileDataSetDescriptionState.LICENSE:
                            self.license = self.license + value
                        elif description_state == PanFileDataSetDescriptionState.SIZE:
                            self.size = self.size + value
                    continue
                elif state == PanFileDataSetState.HEADER:
                    self._header = line.split("\t")
                    columns_map = {}
                    self._columns = []
                    for h in self._header:
                        h = h.split("[", maxsplit=1)[0].strip()
                        if h in columns_map:
                            columns_map[h] = columns_map[h]+1
                            self._columns.append(f"{h}_{columns_map[h]}")
                        else:
                            self._columns.append(h)
                            columns_map[h] = 1
                    state = PanFileDataSetState.DATA
                    break
        self._data = pd.read_csv(
            self.file_path, sep="\t", skiprows=skiprows, names=self._columns)
        self.loaded = True

    @property
    def data(self):
        """
        Get the data set.
        """
        self._load()
        return self._data


class PanCachedDataSet:
    """
    Cached data set from Pangaea.
    """

    def __init__(self, pangaea_index=None, file_path=None, enable_cache=True):
        self.index = pangaea_index
        self.file_path = file_path
        self.enable_cache = enable_cache
        self._data_set = None

    @property
    def data_set(self):
        """
        Get the data set from  local file if existed or from Pangaea website.
        """
        if self._data_set is not None:
            return self._data_set
        if self.index is None and self.file_path is None:
            raise ValueError("neither index nor file_path is set")
        if self.file_path is not None and os.path.exists(self.file_path):
            self._data_set = PanFileDataSet(self.file_path)
        else:
            self._data_set = pgpd.PanDataSet(
                self.index, enable_cache=self.enable_cache, cache_expiry_days=60)
        return self._data_set

    @property
    def data(self):
        """
        PandaDataFrame of our data
        """
        return self.data_set.data

    def print_info(self):
        """
        Print the data set info for debugging purposes.
        """
        print("--------------------")
        print(f"Index: {self.index}")
        print(f"File: {self.file_path}")
        self.data_set.info()

