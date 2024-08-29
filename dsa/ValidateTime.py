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

obj = ValidateTime()
print(obj.solution(1, 8, 3, 2))
print(obj.solution(2, 3, 3, 2))
print(obj.solution(6, 2, 4, 7))