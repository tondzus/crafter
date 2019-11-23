import types
import pytest
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


@pytest.fixture()
def mypipe():
    return crafter.create_pipe('mypipe')


class TestReadme:
    def test_pipeline_creation(self, mypipe):
        assert isinstance(mypipe, Pipeline)

    def test_pipeline_as_attribute(self, mypipe):
        assert isinstance(crafter.mypipe, Pipeline)
        assert crafter.mypipe == mypipe

    def test_register_method(self, mypipe):
        mypipe.register(add_one)

    def test_register_and_run_method(self, mypipe):
        mypipe.register(add_one)
        result = mypipe.process([1, 2, 3])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [2, 3, 4]

    def test_register_generator(self, mypipe):
        mypipe.register(double_even_item)

    def test_register_and_run_generator(self, mypipe):
        mypipe.register(double_even_item)
        result = mypipe.process([1, 2, 3])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [1, 2, 2, 3]

