import types


class Stage:
    def __init__(self, callable):
        self.callable = callable

    def __call__(self, item):
        result = self.callable(item)
        if not isinstance(result, types.GeneratorType):
            yield result
        else:
            yield from result


class Pipeline:
    def __init__(self):
        self.stages = []

    def register(self, callable):
        stage = Stage(callable)
        self.stages.append(stage)

    def process(self, iterable):
        for item in iterable:
            yield from self.depth_first_item_process(0, item)

    def depth_first_item_process(self, stage_number, item):
        if len(self.stages) <= stage_number:
            yield item
            return

        step_callable = self.stages[stage_number]
        for processed_item in step_callable(item):
            yield from self.depth_first_item_process(stage_number + 1, processed_item)
