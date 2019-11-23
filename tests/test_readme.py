import types
from crafter import crafter


mypipe = crafter.create_pipe('mypipe')


@crafter.mypipe.register
def double_even_item(item):
    if item % 2 == 0:
        yield item
    yield item


@mypipe.register(group=1)
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


def test_readme_e2e():
    result = mypipe.process([1, 2, 3, 4, 5, 6])
    assert isinstance(result, types.GeneratorType)
    assert list(result) == [2, 3, 3, 4, 5, 6, 7, 7]
