import os
import logging

from archivekit.util import encode_text

from loadkit.types.stage import Stage
from loadkit.operators.common import TransformOperator

log = logging.getLogger(__name__)

HTML_EXT = ['html', 'htm']
HTML_MIME = 'text/html'
OCR_EXT = ['pdf', 'png', 'jpg', 'jpeg', 'bmp', 'gif']


def text_empty(text):
    if text is None:
        return True
    return len(text.strip()) <= 0


def extract_content(resource):
    if resource.meta.get('extract_article'):
        try:
            from newspaper import Article
            article = Article(resource.meta.get('source_url'))
            article.download(html=resource.data())
            article.parse()
            if article.title and not resource.meta.get('title'):
                resource.meta['title'] = article.title
            if article.text:
                return article.text
        except ImportError:
            log.error('Newspaper is not installed.')
        except Exception, e:
            log.exception(e)

    try:
        from textract.parsers import process
        with resource.local() as file_name:
            text = process(file_name)
            if resource.meta.get('extension') in OCR_EXT and text_empty(text):
                log.info("Using OCR for: %r", resource)
                text = process(file_name, method='tesseract')
        return text
    except ImportError:
        log.error('Textract is not installed.')
    except Exception, e:
        err = unicode(e).split('\n')[0]
        log.error('Textract failed: %s', err)


class TextExtractOperator(TransformOperator):

    DEFAULT_TARGET = os.path.join(Stage.GROUP, 'plain.txt')

    def transform(self, source, target):
        text = extract_content(source)
        if not text_empty(text):
            # TODO: copy metadata?
            target.save_data(encode_text(text))
        source.package.save()
