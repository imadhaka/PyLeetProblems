#LRU Cache Implementation
'''
1. Get a ordered dictionary
2. put -- if cache is full, remove the first key. Add the key to cache and move to end
3. get -- if key is found, return key
'''

from collections import OrderedDict

cache = OrderedDict()

class LRUCacheImpl:
    def __init__(self, size):
        self.size = size

    def put(self, key, value):
        if len(cache) >= self.size:
            cache.popitem(last=False)
        cache[key] = value
        cache.move_to_end(key)

    def get(self, key):
        if key not in cache:
            return 'key not found'
        else:
            cache.move_to_end(key)
            return cache[key]


object = LRUCacheImpl(2)

object.put(1, 'A')
print(cache)

object.put(2, 'B')
print(cache)

res=object.get(1)
print('value for 1 = ',res)
print(cache)

res=object.get(3)
print('value for 3 = ',res)
print(cache)

object.put(3, 'C')
print(cache)

object.put(4, 'D')
print(cache)

res=object.get(2)
print('value for 2 = ',res)
print(cache)

res=object.get(4)
print('value for 4 = ',res)
print(cache)

res=object.get(3)
print('value for 3 = ',res)
print(cache)
