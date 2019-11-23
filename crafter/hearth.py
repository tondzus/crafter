from crafter.pipeline import Pipeline


class Crafter:
    def __init__(self):
        self.pipes = {}

    def create_pipe(self, name):
        self.pipes[name] = Pipeline()
        return self.pipes[name]

    def __getattr__(self, item):
        try:
            super().__getattr__(item)
        except AttributeError:
            if item in self.pipes:
                return self.pipes[item]
            raise
