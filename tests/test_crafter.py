import types
from crafter import crafter
from crafter.pipeline import Pipeline


def add_one(item):
    return item + 1


def double_even_item(item):
    if item % 2 == 0:
        yield item
    yield item


class DropEveryFifthItem:
    def __init__(self):
        self.count = 0

    def __call__(self, item):
        self.count += 1
        if self.count % 5 != 0:
            yield item


class TestReadme:
    def test_pipeline_creation(self):
        pipe = crafter.create_pipe('mypipe')
        assert isinstance(pipe, Pipeline)

    def test_pipeline_as_attribute(self):
        pipe = crafter.create_pipe('mypipe')
        assert isinstance(crafter.mypipe, Pipeline)
        assert crafter.mypipe == pipe

    def test_register_method(self):
        mypipe = crafter.create_pipe('mypipe')
        mypipe.register(add_one)

    def test_register_and_run_method(self):
        mypipe = crafter.create_pipe('mypipe')
        mypipe.register(add_one)
        result = mypipe.process([1, 2, 3])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [2, 3, 4]

