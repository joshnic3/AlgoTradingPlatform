import datetime

from library.bootstrap import Constants
from library.utilities.log import log_hr, get_log_file_path
from library.interfaces.sql_database import Database
import os


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

    def __init__(self):
        self.name = Constants.configs['job_name'] if Constants.configs['job_name'] else Constants.configs['script_name']
        self.id = str(abs(hash(self.name + datetime.datetime.now().strftime(Constants.date_time_format))))
        self.script = Constants.configs['script_name']
        self.phase_name = None

        self._db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

        self._version = Constants.configs['version']

        log_path = get_log_file_path(Constants.configs['logs_root_path'], job_name=self.name)
        self._db.insert_row('jobs', [self.id, self.name, self.script, self._version, log_path, None])
        self.update_phase("INITIATED")

    def log(self, logger=None):
        if logger is None:
            logger = Constants.log
        logger.info('Starting job: {0}'.format(self.id))
        log_hr(logger)

    def _add_phase(self, name):
        phase_id = str(abs(hash(name + self.id)))
        date_time = datetime.datetime.now().strftime(Constants.date_time_format)
        self._db.insert_row('phases', [phase_id, self.id, date_time, name])
        return phase_id

    def update_phase(self, phase):
        self.phase_name = phase.replace(' ', '_').upper()
        phase_id = self._add_phase(self.phase_name)
        self._db.update_value('job', 'phase_id', phase_id, 'id="{0}"'.format(self.id))

    def finished(self, status=None, condition=None):
        log_hr()
        if condition is None:
            self.update_phase('TERMINATED_SUCCESSFULLY')
        else:
            Constants.log.warning('Job finished early. condition: "{0}"'.format(condition))
            self.update_phase('TERMINATED_{0}'.format(condition))

        start_time = self._db.get_one_row('phases', 'job_id="{0}" AND name="INITIATED"'.format(self.id))[2]
        start_time = datetime.datetime.strptime(start_time, Constants.date_time_format)
        run_time = round((datetime.datetime.now() - start_time).total_seconds(), 3)
        self._db.update_value('jobs', 'elapsed_time', run_time, 'id="{0}"'.format(self.id))

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
