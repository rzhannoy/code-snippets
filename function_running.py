import importlib
import json

from loguru import logger


##############
##  DJANGO  ##
##############

# /core/management/commands/run_function.py

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = '''
        Run arbitrary functions by path with optional kwargs:
        manage.py run_function core.utils.gen_random_string
        manage.py run_function core.utils.gen_random_string --kwargs '{"n": 40, "secure": true}'
    '''

    def add_arguments(self, parser):
        parser.add_argument('path', help='Dotted path of the function to run')
        parser.add_argument('--kwargs', default='{}', help='Keyword arguments as a JSON string')

    def handle(self, *args, **options):
        # Add exception handling here, if failing fast in undesired
        module_name, func_name = options['path'].rsplit('.', 1)
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
        kwargs = json.loads(options['kwargs'])

        logger.info(f"Running `{func_name}` with kwargs: {kwargs}")
        output = func(**kwargs)
        logger.success(f"`{func_name}` finished with output: {output}")


#############
##  TYPER  ##
#############

# /core/management/run_function.py

import typer


def main(path: str, kwargs: str = '{}'):
    '''
    Run arbitrary functions by path with optional kwargs:
    python -m core.management.run_function core.utils.gen_random_string
    python -m core.management.run_function core.utils.gen_random_string --kwargs '{"n": 40, "secure": true}'
    '''

    module_name, func_name = path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    kwargs = json.loads(kwargs)

    logger.info(f"Running `{func_name}` with parameters: {kwargs}")
    output = func(**kwargs)
    logger.info(f"`{func_name}` finished with the output: {output}")


if __name__ == '__main__':
    typer.run(main)
