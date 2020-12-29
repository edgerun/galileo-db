import argparse
from datetime import datetime
from galileodb.factory import create_experiment_database_from_env


def delete_exp(args):
    print(f'deleting {args.exp_id}...')
    exp_db = create_experiment_database_from_env()
    exp_db.open()

    try:
        exp_db.delete_experiment(args.exp_id)
        print(f'deleted {args.exp_id}')
    except ValueError:
        print(f'could not find experiment {args.exp_id}')


def show_exp(args):
    print(f'showing {args.exp_id}...')
    exp_db = create_experiment_database_from_env()
    exp_db.open()

    exp = exp_db.get_experiment(args.exp_id)

    if exp:
        print(f'experiment: {exp}')
    else:
        print(f'could not find experiment {args.exp_id}')


def list_exp(args):
    exp_db = create_experiment_database_from_env()
    exp_db.open()

    exps = exp_db.find_all()

    if not exps:
        print(f'no experiments found')

    exps = sorted(exps, key=lambda e: e.created)

    for exp in exps:
        if len(exp.name) > 30:
            name = str(exp.name)[:26] + ' ...'
        else:
            name = exp.name

        created = datetime.fromtimestamp(exp.created)

        line = f'| {exp.id:20s} | {name:31s} | {exp.creator:23s} | {created:%d, %b %Y %H:%M} |'
        print(line)

    print('+' + ('-' * (len(line) - 2)) + '+')


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
    sp_list = sp.add_parser('list', help='list all experiments')
    parser.add_argument('--exp_id', help='the id of the experiment', required=False)

    sp_running_exp = sp.add_parser('get_running_exp_id', help='get the id of the currently running experiment')

    sp_delete.set_defaults(func=delete_exp)
    sp_show.set_defaults(func=show_exp)
    sp_list.set_defaults(func=list_exp)
    sp_running_exp.set_defaults(func=get_running_experiment_id)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
