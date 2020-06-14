import multiprocessing
import sys
import datetime
import pytz

import strategy_executor
import strategy_regression_tester
from library.bootstrap import Constants


class ScriptRunner:

    STRATEGY_EXECUTOR = 'strategy_executor'
    REGRESSION_TESTER = 'strategy_regression_tester'
    SCRIPTS = [REGRESSION_TESTER, STRATEGY_EXECUTOR]

    def __init__(self):
        self._run_time_backup = Constants.run_time
        self._environment_backup = Constants.environment
        self._root_path_backup = Constants.root_path
        self._job_name_backup = Constants.job_name
        self._script_backup = Constants.script
        self._sys_argv_backup = sys.argv

    @staticmethod
    def _swap_out_argv(script, job_name, args_dict):
        # Swap out sys argv.
        to_add = ['-e', Constants.environment, '-r', Constants.root_path, '-x', args_dict['xml_file'], '-j', job_name]

        # Apply any command line args.
        for arg in args_dict:
            to_add.append('--{}'.format(arg))
            to_add.append(args_dict[arg])

        # Swap out script name.
        sys.argv[0] = '{}.py'.format(script)

        for arg in to_add:
            sys.argv.append(arg)

    def _restore_constants_and_argv(self):
        Constants.run_time = self._run_time_backup
        Constants.environment = self._environment_backup
        Constants.root_path = self._root_path_backup
        Constants.job_name = self._job_name_backup
        Constants.script = self._script_backup
        sys.argv = self._sys_argv_backup

    def run(self, script, job_name, args_dict):
        # Override run time.
        Constants.run_time = datetime.datetime.now(pytz.timezone(Constants.TIME_ZONE))

        # Swap out constants and sys argv.
        self._swap_out_argv(script, job_name, args_dict)

        result = -1
        if script.lower() not in self.SCRIPTS:
            raise Exception('{} is not a valid script.'.format(script))

        if script == self.STRATEGY_EXECUTOR:
            # Run main.
            result = strategy_executor.main()

        if script == self.REGRESSION_TESTER:
            # Run main.
            result = strategy_regression_tester.main()

        self._restore_constants_and_argv()
        return result

    def run_asynchronously(self, script, job_name, args_dict):
        process = multiprocessing.Process(target=self.run, args=(script, job_name, args_dict, ))
        process.start()






