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


class TestSingleStageUsage:
    def test_pipeline_creation(self, mypipe):
        assert isinstance(mypipe, Pipeline)
        assert mypipe.name == 'mypipe'

    def test_pipeline_as_attribute(self, mypipe):
        assert isinstance(crafter.mypipe, Pipeline)
        assert crafter.mypipe == mypipe
        assert crafter.mypipe.name == 'mypipe'

    def test_register_method(self, mypipe):
        stage = mypipe.register(add_one)
        assert stage.name == 'add_one'

    def test_register_and_run_method(self, mypipe):
        mypipe.register(add_one)
        result = mypipe.process([1, 2, 3])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [2, 3, 4]

    def test_register_generator(self, mypipe):
        stage = mypipe.register(double_even_item)
        assert stage.name == 'double_even_item'

    def test_register_and_run_generator(self, mypipe):
        mypipe.register(double_even_item)
        result = mypipe.process([1, 2, 3])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [1, 2, 2, 3]

    def test_register_callable_class_instance(self, mypipe):
        stage = mypipe.register(DropEveryFifthItem())
        assert stage.name == 'DropEveryFifthItem'

    def test_register_and_run_callable_class_instance(self, mypipe):
        mypipe.register(DropEveryFifthItem())
        result = mypipe.process([1, 2, 3, 4, 5, 6])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [1, 2, 3, 4, 6]

    def test_register_callable_class(self, mypipe):
        stage = mypipe.register(DropEveryFifthItem)
        assert stage.name == 'DropEveryFifthItem'

    def test_register_and_run_callable_class(self, mypipe):
        mypipe.register(DropEveryFifthItem)
        result = mypipe.process([1, 2, 3, 4, 5, 6])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [1, 2, 3, 4, 6]


class TestMultiStageUsage:
    def test_define_multistage_pipeline(self, mypipe: Pipeline):
        mypipe.register(double_even_item)
        mypipe.register(add_one)
        mypipe.register(DropEveryFifthItem())

    def test_readme_quick_tutorial_pipeline(self, mypipe: Pipeline):
        mypipe.register(double_even_item)
        mypipe.register(add_one)
        mypipe.register(DropEveryFifthItem())
        result = mypipe.process([1, 2, 3, 4, 5, 6])
        assert isinstance(result, types.GeneratorType)
        assert list(result) == [2, 3, 3, 4, 5, 6, 7, 7]


def test_drop_none_items(mypipe: Pipeline):
    drop_even = lambda num: num if num % 2 == 1 else None
    mypipe.register(drop_even)
    result = mypipe.process([1, 2, 3, 4, 5])
    assert list(result) == [1, 3, 5]


class TestGroups:
    def test_register_method_with_group_option(self, mypipe: Pipeline):
        mypipe.register(sum, group_size=2)
        result = mypipe.process([1, 2, 3, 4])
        assert list(result) == [3, 7]

    def test_group_size_bigger_than_input_list(self, mypipe: Pipeline):
        mypipe.register(sum, group_size=6)
        result = mypipe.process([1, 2, 3, 4, 5])
        assert list(result) == [15]

    def test_group_size_is_not_divisible_with_input_size(self, mypipe: Pipeline):
        mypipe.register(sum, group_size=6)
        result = mypipe.process([1, 2, 3, 4, 5, 6, 7])
        assert list(result) == [21, 7]
