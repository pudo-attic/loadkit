import logging

import yaml
import click
from archivekit import open_collection

from loadkit.util import LoadKitException
from loadkit.pipeline import Pipeline


log = logging.getLogger(__name__)


def execute_pipeline(ctx, fh, operation):
    try:
        config = yaml.load(fh.read())
        fh.close()
    except Exception, e:
        raise click.ClickException("Cannot parse pipeline: %s" % e)
    if 'config' not in config:
        config['config'] = {}

    collections = ctx.pop('collections', [])
    config['config'].update(ctx)
    config['config']['threads'] = ctx.pop('threads', None)

    collection_configs = config['config'].pop('collections', {})
    if not len(collections):
        collections = collection_configs.keys()
    collections = [c for c in collections if c in collection_configs]

    for cname in collections:
        cconfig = collection_configs.get(cname)
        coll = open_collection(cname, cconfig.pop('type'), **cconfig)
        try:
            pipeline = Pipeline(coll, fh.name, config=config)
            getattr(pipeline, operation)()
        except LoadKitException, de:
            raise click.ClickException(unicode(de))


@click.group()
@click.option('-c', '--collections', default=None, nargs=-1,
              help='The configured collection name to use.')
@click.option('-t', '--threads', default=None, type=int,
              help='Number of threads to process data')
@click.option('-d', '--debug', default=False, is_flag=True,
              help='Verbose output for debugging')
@click.pass_context
def cli(ctx, collections, threads, debug):
    """ A configurable data and document processing tool. """
    ctx.obj = {
        'collections': collections,
        'debug': debug,
        'threads': threads
    }
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)


@cli.command()
@click.argument('pipeline', type=click.File('rb'))
@click.pass_obj
def run(ctx, pipeline):
    """ Execute the given PIPELINE. """
    execute_pipeline(ctx, pipeline, 'run')


@cli.command()
@click.argument('pipeline', type=click.File('rb'))
@click.pass_obj
def extract(ctx, pipeline):
    """ Execute the extractors in PIPELINE. """
    execute_pipeline(ctx, pipeline, 'extract')


@cli.command()
@click.argument('pipeline', type=click.File('rb'))
@click.pass_obj
def transform(ctx, pipeline):
    """ Execute the transformers in PIPELINE. """
    execute_pipeline(ctx, pipeline, 'transform')
