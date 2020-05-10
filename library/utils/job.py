import datetime

import library.bootstrap as globals
from library.utils.log import log_hr


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

    def __init__(self, configs, db):
        self.name = configs['job_name'] if configs['job_name'] else 'manual_run'
        self.script = configs['script_name']
        self.start_time = datetime.datetime.now()
        self.status = None

        self._db = db
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
            self._db.insert_row('jobs', [self.id, self.name, self.script, self._version, datetime.datetime.now(), status])
        self.status = status

    def log(self, logger=None):
        if logger is None:
            logger = globals.log
        logger.info('Starting job: {0}'.format(self.__str__()))
        log_hr(logger)

    def update_status(self, status):
        self._set_status(status.upper())

    def finished(self, logger=None, status=None):
        if logger is None:
            logger = globals.log
        log_hr(logger)
        self.update_status('COMPLETED')
        run_time = (datetime.datetime.now() - self.start_time).total_seconds()
        if status:
            status_map = {0: "SUCCESSFULLY",
                          1: "with ERRORS",
                          2: "with WARNINGS"}

            if status in status_map:
                logger.info('Job "{0}" finished {1} in {2} seconds.'.format(self.name, status_map[status], run_time))
            else:
                logger.info('Job {0} failed with status {1} after {2} seconds!'.format(self.name, status_map[status],
                                                                                       status))
        else:
            logger.info('Job "{0}" finished in {1} seconds.'.format(self.name, run_time))
