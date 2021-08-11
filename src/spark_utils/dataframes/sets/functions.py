"""
 Helper functions for set operations
"""

from typing import Iterable


def case_insensitive_diff(left_set: Iterable[str], right_set: Iterable[str]) -> Iterable[str]:
    """
      Computes a difference between two collection of string, ignoring case

    :param left_set: first collection
    :param right_set: second collection
    :return:
    """
    right_set_lower = list(map(lambda x: x.lower(), right_set))
    for left in left_set:
        if left.lower() not in right_set_lower:
            yield left
