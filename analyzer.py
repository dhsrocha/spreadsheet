#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Creates insightful structured ata based on provided spreadsheet's contents."""

__author__ = 'Diego Rocha'
__maintainer__ = __author__
__email__ = 'dhsrocha.dev@gmail.com'
__version__ = '0.1.0-SNAPSHOT'

import sys
from typing import Type


class _Analyzer:
    """Application entrypoint."""

    @staticmethod
    def _throw(ex: Type[Exception], message: str) -> None: raise ex(message)

    if __name__ == '__main__':
        sys.version_info < (3, 12) and _throw(RuntimeError, 'Minimum version is 3.12')
        print('Hello World!')
