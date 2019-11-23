class Pipeline:
    def __init__(self):
        self.steps = []

    def register(self, step):
        self.steps.append(step)

    def process(self, iterable):
        for item in iterable:
            for step in self.steps:
                item = step(item)
            yield item
