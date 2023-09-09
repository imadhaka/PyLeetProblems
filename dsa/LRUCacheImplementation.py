#LRU Cache Implementation
'''
1. Create a Map<Integer, string>
2. Create a list for order

3. put  -- if size is full, remove the last & add key to list at first
        -- if size is not full, add key to list at first
'''
from collections import OrderedDict

class LRUCacheImpl:
    def __init__(self, size):
        self.size = size
        self.cache = OrderedDict()

    def put(self, key, value):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.size:
            self.cache.popitem(last=False)

    def get(self, key):
        if key not in self.cache:
            return -1
        else:
            self.cache.move_to_end(key)
            return self.cache[key]


cache = LRUCacheImpl(2)

cache.put(1, 1)
print(cache.cache)
cache.put(2, 2)
print(cache.cache)
cache.get(1)
print(cache.cache)
cache.put(3, 3)
print(cache.cache)
cache.get(2)
print(cache.cache)
cache.put(4, 4)
print(cache.cache)
cache.get(1)
print(cache.cache)
cache.get(3)
print(cache.cache)
cache.get(4)
print(cache.cache)