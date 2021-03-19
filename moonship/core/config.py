#  Copyright (c) 2021, Marlon Paulse
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#
#  1. Redistributions of source code must retain the above copyright notice, this
#     list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import yaml

from collections import ItemsView
from typing import Iterator, Union

__all__ = [
    "Config",
    "ConfigItemsView"
]


def convert_config_value(value: any, parent_key: str, key: str) -> any:
    if isinstance(value, dict):
        value = Config(value, f"{parent_key}.{key}" if parent_key is not None else key)
    elif isinstance(value, float):
        value = str(value)
    return value


class ConfigItemsView(ItemsView):

    def __init__(self, config_dict: dict, key: str):
        super().__init__(config_dict)
        self.key = key

    def __contains__(self, item) -> bool:
        key, value = item
        try:
            v = self._mapping[key]
        except KeyError:
            return False
        else:
            return v is value or v == value

    def __iter__(self) -> Iterator[tuple[str, Union["Config", any]]]:
        for key in self._mapping:
            value = convert_config_value(self._mapping[key], self.key, key)
            yield key, value


class Config:

    def __init__(self, config_dict: dict, key: str = None) -> None:
        self.dict = config_dict
        self.key = key

    def __ior__(self, other: "Config") -> "Config":
        self.dict |= other.dict
        return self

    def __iter__(self) -> Iterator[any]:
        return iter(self.dict)

    def items(self) -> ItemsView[str, Union["Config", any]]:
        return ConfigItemsView(self.dict, self.key)

    def get(self, key: str, default: any = None) -> Union["Config", any]:
        keys = key.split(".")
        value = self.dict
        for i in range(0, len(keys)):
            value = value.get(keys[i])
            if value is None or (not isinstance(value, dict) and i < len(keys) - 1):
                return default
        return convert_config_value(value, self.key, key)

    @staticmethod
    def load_from_file(config_filename: str) -> "Config":
        config = {
            "moonship": {
            }
        }
        if os.path.isfile(config_filename):
            with open(config_filename, "r") as config_file:
                config = yaml.safe_load(config_file)
        return Config(config)
