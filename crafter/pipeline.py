import types


class Step:
    def __init__(self, callable):
        self.callable = callable

    def __call__(self, item):
        result = self.callable(item)
        if not isinstance(result, types.GeneratorType):
            yield result
        else:
            yield from result


class PipelineExecution:
    def __init__(self, steps):
        self.steps = steps

    def process_item(self, step_number, item):
        if len(self.steps) <= step_number:
            yield item
            return

        step_callable = self.steps[step_number]
        for processed_item in step_callable(item):
            yield from self.process_item(step_number + 1, processed_item)


class Pipeline:
    def __init__(self):
        self.steps = []

    def register(self, callable):
        step = Step(callable)
        self.steps.append(step)

    def process(self, iterable):
        execution = PipelineExecution(self.steps)
        for item in iterable:
            yield from execution.process_item(0, item)
