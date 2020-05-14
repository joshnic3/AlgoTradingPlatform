import datetime

from library.bootstrap import Constants
from library.utilities.log import log_hr, get_log_file_path
from library.interfaces.sql_database import Database


def get_run_count(db, script_name, version=None):
    version_index = 3

    condition = 'script="{0}"'.format(script_name.lower())
    results = db.query_table('jobs', condition)
    if version:
        versions = set([r[version_index] for r in results])
        version_to_count = max(versions) if version.lower() == 'latest' else version
        return len([r for r in results if r[version_index] == version_to_count])
    else:
        return len([r for r in results])


def is_script_new(db, script_name):
    new_threshold = 50
    no_of_runs_on_latest_version = get_run_count(db, script_name, 'latest')
    if no_of_runs_on_latest_version < new_threshold:
        return True
    return False


class Job:
    # Maybe add a job phase table.

    def __init__(self, configs):
        self.name = configs['job_name'] if configs['job_name'] else 'manual_run'
        self.script = configs['script_name']
        self.start_time = datetime.datetime.now()
        self.status = None

        self._db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])
        self._parameters = ''
        self.id = str(abs(hash(self.name + self.start_time.strftime('%Y%m%d%H%M%S'))))
        self._version = configs['version']
        self._set_status('INITIATED')

    def __str__(self):
        is_new = is_script_new(self._db, self.script)
        return '[id: {0}, job: {1}, script: {2}, code version: {3}]'.format(self.id,
                                                                            self.name,
                                                                            self.script,
                                                                            '{0}{1}'.format(self._version,
                                                                                            '(NEW)' if is_new else ''))

    def _set_status(self, status):
        if self.status:
            self._db.update_value('jobs', 'status', status, 'id="{}"'.format(self.id))
            self._db.update_value('jobs', 'date_time', datetime.datetime.now(), 'id="{}"'.format(self.id))
        else:
            log_path = get_log_file_path(Constants.configs['logs_root_path'], job_name=Constants.configs['job_name'])
            self._db.insert_row('jobs', [self.id, self.name, self.script, self._version, datetime.datetime.now(), status, log_path])
        self.status = status

    def log(self, logger=None):
        if logger is None:
            logger = Constants.log
        logger.info('Starting job: {0}'.format(self.__str__()))
        log_hr(logger)

    def update_status(self, status):
        # TODO replace with phase
        #     add phase table and like with job id
        #     this way we will only add rows to both tables
        #     with maybe exception of saving the log path
        #     job table can have name, script, verison, log_path, _time
        #     Will use phase table to calc elapsed_run/start/end times
        self._set_status(status.upper())

    def terminate(self, condition=None):
        if condition is None:
            self.update_status('TERMINATED_SUCCESSFULLY')
        else:
            self.update_status('TERMINATED_{0}'.format(condition))

    def finished(self, status=None):
        log_hr()
        self.terminate()
        run_time = (datetime.datetime.now() - self.start_time).total_seconds()
        # TODO save runtime to db
        if status:
            status_map = {0: "SUCCESSFULLY",
                          1: "with ERRORS",
                          2: "with WARNINGS"}

            if status in status_map:
                Constants.log.info(
                    'Job "{0}" finished {1} in {2} seconds.'.format(self.name, status_map[status], run_time))
            else:
                Constants.log.info(
                    'Job {0} failed with status {1} after {2} seconds!'.format(self.name, status_map[status],
                                                                               status))
        else:
            Constants.log.info('Job "{0}" finished in {1} seconds.'.format(self.name, run_time))
