import datetime

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
        self.name = configs['job_name']
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

    def log(self, log):
        log.info('Starting job: {0}'.format(self.__str__()))

    def update_status(self, status):
        self._set_status(status.upper())

    def finished(self, log=None, status=None):
        # TODO Calculate average run time and log warn if it is longer.
        log_hr(log)
        self.update_status('COMPLETED')
        if log:
            end_time = datetime.datetime.now()
            run_time = (end_time - self.start_time).total_seconds()

            if status:
                status_map = {0: "SUCCESSFULLY",
                              1: "with ERRORS",
                              2: "with WARNINGS"}

                if status in status_map:
                    log.info('Job "{0}" finished {1} in {2} seconds.'.format(self.name, status_map[status], run_time))
                else:
                    log.info('Job {0} failed with status {1} after {2} seconds!'.format(self.name, status_map[status],
                                                                                        status))
            else:
                log.info('Job "{0}" finished in {1} seconds.'.format(self.name, run_time))


