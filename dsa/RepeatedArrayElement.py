"""
You are given an integer array nums with the following properties:
nums.length == 2 * n.
nums contains n + 1 unique elements.
Exactly one element of nums is repeated n times.
Return the element that is repeated n times.

"""
from typing import List

class Solution:
    def repeatedNTimes(self, nums: List[int]) -> int:
        print(nums)
        myset = set([])
        repeat = 0
        for i in nums:
            print(i)
            if myset.__contains__(i):
                repeat = i
            myset.add(i)
        return repeat

nums = [2, 1, 2, 5, 3, 2]
s1 = Solution()
print("Repeated Element is: " + str(s1.repeatedNTimes(nums)))