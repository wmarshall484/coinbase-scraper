'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK, TICKER, TRADES
from cryptofeed.exchanges import Coinbase
from cryptofeed.util.async_file import AsyncFileCallback

import atexit
from collections import defaultdict

from aiofile import AIOFile
import json
import sys

sym = sys.argv[1]

class ImmediateWriteAsyncFileCallback:
    def __init__(self, path, length=1024, rotate=1024 * 1024 * 1024 * 10):
        self.path = path
        self.length = length
        self.data = []
        self.rotate = rotate
        self.pointer = 0
        self.file_counter = 0

    async def __call__(self, datum: str, timestamp: float, uuid: str):
        datum = json.loads(datum)
        if not 'maker_order_id' in datum:
            return
        datum.pop('type')
        datum.pop('maker_order_id')
        datum.pop('taker_order_id')
        datum = json.dumps(datum)

        self.data.append(datum)
        print(f'{sym} data is ', len(self.data), ' out of ', self.length)
        if len(self.data) > self.length:
            p = f"{self.path}/{sym}_USD.{uuid}.{self.file_counter}"
            async with AIOFile(p, mode='a') as fp:
                print('writing file', p)
                r = await fp.write('\n'.join([f'{d}' for d in self.data]) + '\n', offset=self.pointer)
                self.pointer += r
                self.data = []
                print('file written. pointer is ', self.pointer, ' out of ', self.rotate)
            
                if self.pointer >= self.rotate:
                    print('rotating file')
                    self.pointer = 0
                    self.file_counter += 1


def main():
    f = FeedHandler(raw_message_capture=ImmediateWriteAsyncFileCallback('.'), handler_enabled=False)
    f.add_feed(Coinbase(symbols=[f'{sym}-USD'], channels=[TRADES]))

    f.run()


if __name__ == '__main__':
    main()
