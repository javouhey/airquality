from abc import ABCMeta, abstractmethod


class ReadingParser(object):

    """Base class for parser of air quality readings.

    Subclasses must define the method `parse`
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def parse(self, **kwargs):
        """Returns a dictonary that maps to the JSON
        which will be persisted to mongo.

        This function MUST be idempotent.
        """
        raise NotImplementedError('Move on. Nothing to see here')
