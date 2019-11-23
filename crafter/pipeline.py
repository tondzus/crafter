import types


class Stage:
    def __init__(self, callable, group=None):
        if group is not None and not isinstance(group, int):
            raise ValueError("Invalid argument group=%s - group must be int!" % group)
        if isinstance(callable, type):
            callable = callable()
        self.callable = callable
        self.group = group

    def __call__(self, item):
        result = self.callable(item)
        if not isinstance(result, types.GeneratorType):
            yield result
        else:
            yield from result


class Pipeline:
    def __init__(self):
        self.stages = []

    def register(self, callable=None, *args, group=None):
        if callable is not None:
            # direct invocation
            stage = Stage(callable, group)
            self.stages.append(stage)
            return stage

        # decorator invocation
        def post_register(decorated_callable):
            stage = Stage(decorated_callable, group)
            self.stages.append(stage)
            return stage

        return post_register

    def process(self, iterable):
        for item in iterable:
            yield from self._depth_first_item_process(0, item)

    def _depth_first_item_process(self, stage_number, item):
        if len(self.stages) <= stage_number:
            yield item
            return

        step_callable = self.stages[stage_number]
        for processed_item in step_callable(item):
            yield from self._depth_first_item_process(stage_number + 1, processed_item)
