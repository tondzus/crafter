import types
import logging


class PipelineStop:
    pass


def default_exception_handler(stage, item, exc: Exception):
    stage.log.error("Stage %s couldn't process item '%': %s", stage, item, exc)


class Stage:
    logger_name = 'crafter.stage.%s'

    def __init__(self, callable):
        try:
            self.name = callable.__name__
        except AttributeError:
            self.name = callable.__class__.__name__

        self.log = logging.getLogger(self.logger_name % self.name)
        self.log.info("Creating stage '%s'", self.name)

        if isinstance(callable, type):
            self.log.debug("Stage '%s' is of type class - instantiating!", self.name)
            callable = callable()

        self.callable = callable
        self.exception_handler = default_exception_handler

    def __call__(self, item):
        if isinstance(item, PipelineStop):
            yield item
            return

        try:
            result = self.callable(item)
        except Exception as exc:
            result = self.exception_handler(self, item, exc)
            if result is None:
                return

        if not isinstance(result, types.GeneratorType):
            self.log.debug("Stage %s (function) transformed '%s' to '%s'", self.name, item, result)
            yield result
            return

        result = iter(result)
        while True:
            try:
                generated_result = next(result)
            except StopIteration:
                break
            except Exception as exc:
                generated_result = self.exception_handler(self, item, exc)
                if generated_result is None:
                    continue
                else:
                    self.log.debug("Stage %s (generator) transformed '%s' to '%s'",
                                   self.name, item, generated_result)
                    yield generated_result
            else:
                self.log.debug("Stage %s (generator) transformed '%s' to '%s'",
                               self.name, item, generated_result)
                yield generated_result

    def exception(self, handler):
        self.exception_handler = handler

    def __str__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.name)


class GroupStage(Stage):
    def __init__(self, callable, group_size=None):
        super().__init__(callable)

        if group_size is not None and not isinstance(group_size, int):
            msg = "Invalid argument group=%s - group must be int!" % group_size
            self.log.error(msg)
            raise ValueError(msg)

        self.group_size = group_size
        self.group = []

    def __call__(self, item):
        if isinstance(item, PipelineStop):
            self.log.debug("GroupStage %s received PipelineStop - flushing buffered items",
                           self.name)
            if self.group:
                yield from super().__call__(self.group)
                self.group.clear()
            yield item
            return

        self.log.debug("GroupStage %s adding %d. item into the buffer: %s",
                       self.name, len(self.group) + 1, item)
        self.group.append(item)
        if self.group_size <= len(self.group):
            self.log.debug("GroupStage %s has filled the buffer - executing stage for %s items",
                           self.name, self.group)
            yield from super().__call__(self.group)
            self.group.clear()

    def __str__(self):
        return '<%s(%d): %s>' % (self.__class__.__name__, self.group_size, self.name)


def create_stage(callable, group_size=None):
    if group_size is None:
        return Stage(callable)
    elif group_size == 1:
        return Stage(callable)
    else:
        return GroupStage(callable, group_size)


class Pipeline:
    def __init__(self, name):
        self.name = name
        self.log = logging.getLogger('crafter.pipeline.%s' % self.name)
        self.stages = []

    def register(self, callable=None, *args, group_size=None):
        if callable is not None:
            # direct invocation
            stage = create_stage(callable, group_size)
            self.log.info("Registering %d. stage - %s", len(self.stages) + 1, stage)
            self.stages.append(stage)
            return stage

        # decorator invocation
        def post_register(decorated_callable):
            stage = create_stage(decorated_callable, group_size)
            self.log.info("Registering %d. stage - %s", len(self.stages) + 1, stage)
            self.stages.append(stage)
            return stage

        return post_register

    def process(self, iterable):
        self.log.info("Starting the pipeline with %s stages!", self.stages)

        for index, item in enumerate(iterable):
            self.log.debug("%d. item submitted into the pipeline: %s", index, item)
            if item is None:
                continue
            yield from self._depth_first_item_process(0, item)

        self.log.info("Last item processed - flushing buffers of group stages")
        for result in self._depth_first_item_process(0, PipelineStop()):
            if isinstance(result, PipelineStop):
                continue
            yield result

    def _depth_first_item_process(self, stage_number, item):
        if len(self.stages) <= stage_number:
            self.log.debug("Returning pipeline result '%s'", item)
            yield item
            return

        stage_callable = self.stages[stage_number]
        self.log.debug("Processing item '%s' in the pipeline with %d. stage %s",
                       item, stage_number, stage_callable.name)

        for processed_item in stage_callable(item):
            if processed_item is None:
                continue

            yield from self._depth_first_item_process(stage_number + 1, processed_item)
