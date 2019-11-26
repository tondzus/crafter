import types
import pytest
from crafter import crafter


mypipe = crafter.create_pipe('mypipe')


@crafter.mypipe.register
def double_even_item(item):
    if item % 2 == 0:
        yield item
    yield item


@mypipe.register(group_size=1)
def add_one(item):
    return item + 1


@mypipe.register
class DropEveryFifthItem:
    def __init__(self):
        self.count = 0

    def __call__(self, item):
        self.count += 1
        if self.count % 5 != 0:
            yield item


@mypipe.register(group_size=6)
def sum_by_6(num_grp):
    return sum(num_grp)


def test_readme_e2e():
    result = mypipe.process([1, 2, 3, 4, 5, 6])
    assert isinstance(result, types.GeneratorType)
    # before the sum_by_6 stage [2, 3, 3, 4, 5, 6, 7, 7]
    assert list(result) == [23, 14]


test_pipe = crafter.create_pipe('test_pipe')


@test_pipe.register
def divide_ten_by(item):
    if item == 3:
        raise ValueError("Can't use 3 to divide!")
    return 10 / item


@divide_ten_by.exception
def divide_by_zero_handler(stage, item, exc):
    if item == 0:
        return None
    else:
        raise exc


def test_exception_handler_works():
    result = test_pipe.process([2, 1, 0])
    assert list(result) == [5, 10]


def test_exception_handler_can_raise():
    with pytest.raises(ValueError) as exc:
        list(test_pipe.process([3]))
    assert exc.errisinstance(ValueError)
    assert str(exc.value) == "Can't use 3 to divide!"
