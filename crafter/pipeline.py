import types


class PipelineStop:
    pass


class Stage:
    def __init__(self, callable):
        if isinstance(callable, type):
            callable = callable()
        self.callable = callable

    def __call__(self, item):
        if isinstance(item, PipelineStop):
            yield item
            return

        result = self.callable(item)
        if not isinstance(result, types.GeneratorType):
            yield result
        else:
            yield from result


class GroupStage(Stage):
    def __init__(self, callable, group_size=None):
        if group_size is not None and not isinstance(group_size, int):
            raise ValueError("Invalid argument group=%s - group must be int!" % group_size)

        super().__init__(callable)
        self.group_size = group_size
        self.group = []

    def __call__(self, item):
        if isinstance(item, PipelineStop):
            if self.group:
                yield from super().__call__(self.group)
                self.group.clear()
            yield item
            return

        self.group.append(item)
        if self.group_size <= len(self.group):
            yield from super().__call__(self.group)
            self.group.clear()


def create_stage(callable, group_size=None):
    if group_size is None:
        return Stage(callable)
    elif group_size == 1:
        return Stage(callable)
    else:
        return GroupStage(callable, group_size)


class Pipeline:
    def __init__(self):
        self.stages = []

    def register(self, callable=None, *args, group_size=None):
        if callable is not None:
            # direct invocation
            stage = create_stage(callable, group_size)
            self.stages.append(stage)
            return stage

        # decorator invocation
        def post_register(decorated_callable):
            stage = create_stage(decorated_callable, group_size)
            self.stages.append(stage)
            return stage

        return post_register

    def process(self, iterable):
        for item in iterable:
            if item is None:
                continue
            yield from self._depth_first_item_process(0, item)

        for result in self._depth_first_item_process(0, PipelineStop()):
            if isinstance(result, PipelineStop):
                continue
            yield result

    def _depth_first_item_process(self, stage_number, item):
        if len(self.stages) <= stage_number:
            yield item
            return

        step_callable = self.stages[stage_number]
        for processed_item in step_callable(item):
            if processed_item is None:
                continue

            yield from self._depth_first_item_process(stage_number + 1, processed_item)
