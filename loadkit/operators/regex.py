import re
import logging

from loadkit.operators.common import SourceOperator

log = logging.getLogger(__name__)


class RegExOperator(SourceOperator):

    @property
    def re(self):
        if not hasattr(self, '_re'):
            term = self.config.get('term', [])
            terms = self.config.get('terms', [term])
            self._re = re.compile('(%s)' % '|'.join(terms))
        return self._re

    def analyze(self, source):
        matches_field = self.config.get('field_matches', 'matches')
        total_field = self.config.get('field_total')

        matches = {}
        for match in self.re.findall(source.fh().read()):
            score = matches.get(match, 0)
            matches[match] = score + 1

        if total_field is not None:
            source.meta[total_field] = sum(matches.values())
        
        source.meta[matches_field] = matches
        source.meta.save()
