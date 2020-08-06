import argparse
import logging

from galileodb.factory import create_experiment_database_from_env

logger = logging.getLogger(__name__)


def delete_exp(args):
    logger.info(f'deleting {args.exp_id}...')
    exp_db = create_experiment_database_from_env()
    exp_db.open()

    try:
        exp_db.delete_experiment(args.exp_id)
        logger.info(f'deleted {args.exp_id}')
    except ValueError:
        logger.info(f'could not find experiment {args.exp_id}')


def show_exp(args):
    logger.info(f'showing {args.exp_id}...')
    exp_db = create_experiment_database_from_env()
    exp_db.open()

    exp = exp_db.get_experiment(args.exp_id)

    if exp:
        logger.info(f'experiment: {exp}')
    else:
        logger.info(f'could not find experiment {args.exp_id}')


def get_running_experiment_id(args):
    exp_db = create_experiment_database_from_env()
    exp_db.open()

    exp = exp_db.get_running_experiment()

    if exp:
        print(f'{exp.id}')
    else:
        print()


def main():
    parser = argparse.ArgumentParser()
    sp = parser.add_subparsers()
    sp_delete = sp.add_parser('delete', help='delete the experiment')
    sp_show = sp.add_parser('show', help='show the experiment')
    parser.add_argument('--exp_id', help='the id of the experiment', required=False)

    sp_running_exp = sp.add_parser('get_running_exp_id', help='get the id of the currently running experiment')

    sp_delete.set_defaults(func=delete_exp)
    sp_show.set_defaults(func=show_exp)
    sp_running_exp.set_defaults(func=get_running_experiment_id)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
