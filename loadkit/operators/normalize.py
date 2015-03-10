import os
import logging
from normality import normalize

from loadkit.types.stage import Stage
from loadkit.operators.common import TransformOperator

log = logging.getLogger(__name__)


class NormalizeOperator(TransformOperator):
    """ Simplify a piece of text to generate a more canonical
    representation. This involves lowercasing, stripping trailing
    spaces, removing symbols, diacritical marks (umlauts) and
    converting all newlines etc. to single spaces.
    """

    DEFAULT_SOURCE = os.path.join(Stage.GROUP, 'plain.txt')
    DEFAULT_TARGET = os.path.join(Stage.GROUP, 'normalized.txt')

    def transform(self, source, target):
        text = source.data()
        text = normalize(text, lowercase=self.config.get('lowercase', True),
                         transliterate=self.config.get('transliterate', False),
                         collapse=self.config.get('collapse', True))
        target.save_data(text.encode('utf-8'))
