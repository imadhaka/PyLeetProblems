"""
Given four digits, count how many valid times can be displayed on a digital clock (in 24-hour format) using those digits.
The earliest time is 00:00 and the latest time is 23:59.

Write a function.

def solution(A, B, C, D)

that, given four integers A, B, C and D (describing the four digits), returns the number of valid times that can be displayed on a digital clock.

Examples:
1. Given A = 1 B = 8 c = 3 D = 2 the function should return 6. The valid times are: 12:38, 13:28, 18:23, 18:32, 21:38 and 23:18.
2. Given A = 2 B = 3 C = 3 D = 2 the function should return 3. The valid times are: 22:33, 23:23 and 23:32.
3. Given A6, B = 2 2c = 4D = 7 , the function should return 0. It is not possible to display any valid time using the given digits.

Assume that:
A, B, C and D are integers within the range [0..9]
"""

from itertools import permutations
class ValidateTime:
    def solution(self, A, B, C, D):
        # Check if all input digits are within the valid range [0, 9]
        if not all(0 <= digit <= 9 for digit in [A, B, C, D]):
            raise ValueError("All digits must be in the range [0, 9]")

        digits = [A, B, C, D]
        valid_times = set()

        for perm in permutations(digits):
            hours = perm[0] * 10 + perm[1]
            minutes = perm[2] * 10 + perm[3]

            if 0 <= hours < 24 and 0 <= minutes < 60:
                valid_times.add((hours, minutes))

        return len(valid_times)

    def solution2(self, A, B, C, D):
        # Check if all input digits are within the valid range [0, 9]
        if not all(0 <= digit <= 9 for digit in [A, B, C, D]):
            raise ValueError("All digits must be in the range [0, 9]")

        digits = [A, B, C, D]
        valid_times = set()

        self.permute(digits, 0, valid_times)
        return len(valid_times)

    def permute(self, digits, start, valid_times):
        if start == len(digits):
            hours = digits[0] * 10 + digits[1]
            minutes = digits[2] * 10 + digits[3]
            if 0 <= hours < 24 and 0 <= minutes < 60:
                valid_times.add((hours, minutes))
        else:
            for i in range(start, len(digits)):
                self.swap(digits, start, i)
                self.permute(digits, start + 1, valid_times)
                self.swap(digits, start, i) # Backtrack

    def swap(self, digits, i, j):
        #temp = digits[i]
        #digits[i] = digits[j]
        #digits[j] = temp
        digits[i], digits[j] = digits[j], digits[i]


obj = ValidateTime()
print(obj.solution(1, 8, 3, 2))
print(obj.solution(2, 3, 3, 2))
print(obj.solution(6, 2, 4, 7))

print(obj.solution2(1, 8, 3, 2))
print(obj.solution2(2, 3, 3, 2))
print(obj.solution2(6, 2, 4, 7))